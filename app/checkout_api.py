r"""checkout-api — the one application every rooca integration observes.

The point of this service is not that it is realistic. It is that its failures
are **layered**: a single request touches a database on a Proxmox guest, an
object store on Ceph, and a payments service on k3s, in that order. Break any one
of those and the symptom is identical from the outside — checkout-api returns
5xx, latency rises, the Prometheus alert fires, Graylog fills with errors, both
APM planes show the same span failing, Sentry gets an exception, Zabbix marks the
host unhealthy and Datadog agrees with all of them.

Nine planes reporting one symptom, one of them caused by the actual fault. That
is the whole measurement: rooca has to name the layer, not the symptom.

Every dependency is optional. A dependency that is not configured is reported as
`skipped`, never as healthy — a checkout that "succeeded" because it never
reached the database would make every scenario built on it vacuous.

Configuration comes from three env files, in this order:
    /etc/checkout-api/estate.env    where every signal goes (written by cloud-init)
    /etc/checkout-api/secrets.env   API keys (written by provision/deploy_app.sh)
    /etc/checkout-api/fault.env     scenario knobs (written by the fault injector)
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import socket
import sys
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
import uvicorn
from fastapi import FastAPI, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

SERVICE = os.getenv("SERVICE_NAME", "checkout-api")
ENV = os.getenv("SERVICE_ENV", "lab")
VERSION = os.getenv("SERVICE_VERSION", "1.0.0")
HOSTNAME = socket.gethostname()

# ── Dependencies ─────────────────────────────────────────────────────────────
# Each is a URL or DSN, or empty. Empty means "not wired yet", which is reported
# as skipped — never silently treated as success.
DEP_DB = os.getenv("DEP_DATABASE_URL", "")        # Postgres on a Proxmox guest
DEP_OBJECT = os.getenv("DEP_OBJECT_URL", "")      # Ceph RGW / S3 endpoint
DEP_PAYMENTS = os.getenv("DEP_PAYMENTS_URL", "")  # payments service on k3s

DEP_TIMEOUT = float(os.getenv("DEP_TIMEOUT_SECONDS", "3.0"))

# ── Fault knobs ──────────────────────────────────────────────────────────────
# Read per-request, not at import: the injector rewrites fault.env and restarts
# the unit, but a scenario that only touches the file should still take effect.
def fault(name: str, default: str = "") -> str:
    return os.getenv(f"FAULT_{name.upper()}", default)


# ── Metrics ──────────────────────────────────────────────────────────────────
# Names match what prometheus.yaml's alert rules expect. `service` is a label
# rather than part of the name so the same rule works for any service added later.
REQS = Counter(
    "http_requests_total", "HTTP requests", ["service", "env", "method", "path", "status"]
)
LAT = Histogram(
    "http_request_duration_seconds", "HTTP request latency",
    ["service", "env", "path"],
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)
DEP_LAT = Histogram(
    "checkout_dependency_duration_seconds", "Dependency call latency",
    ["service", "dependency", "outcome"],
    buckets=(0.005, 0.025, 0.1, 0.5, 1.0, 3.0, 10.0),
)
DEP_UP = Gauge("checkout_dependency_up", "1 when the dependency last answered", ["dependency"])
BUILD = Gauge("checkout_build_info", "Build info", ["service", "version", "env"])
BUILD.labels(SERVICE, VERSION, ENV).set(1)

log = logging.getLogger(SERVICE)


# ── Logging: stdout (journald -> Alloy/Loki) + GELF (Graylog) ────────────────
def setup_logging() -> None:
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(logging.Formatter(
        '{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s",'
        f'"service":"{SERVICE}","env":"{ENV}","host":"{HOSTNAME}",'
        '"msg":"%(message)s"}'
    ))
    root.addHandler(stream)

    host = os.getenv("GRAYLOG_HOST")
    if not host:
        return
    try:
        from pygelf import GelfUdpHandler
        # Static fields land as first-class Graylog fields, which is what makes
        # the graylog plugin able to scope a search to this service at all.
        root.addHandler(GelfUdpHandler(
            host=host,
            port=int(os.getenv("GRAYLOG_GELF_UDP_PORT", "12201")),
            debug=True,
            include_extra_fields=True,
            _service=SERVICE,
            _env=ENV,
            _version=VERSION,
        ))
        log.info("gelf handler attached to %s", host)
    except Exception as exc:  # noqa: BLE001 — a missing log sink must not stop the service
        print(f"gelf handler unavailable: {exc}", file=sys.stderr)


# ── Sentry ───────────────────────────────────────────────────────────────────
def setup_sentry() -> None:
    dsn = os.getenv("SENTRY_DSN")
    if not dsn:
        return
    try:
        import sentry_sdk
        sentry_sdk.init(
            dsn=dsn,
            environment=ENV,
            release=f"{SERVICE}@{VERSION}",
            traces_sample_rate=1.0,
            # The scenarios care about which release broke things, so the release
            # has to be attached even when the exception is caught and turned into
            # a 5xx rather than propagating.
            attach_stacktrace=True,
        )
        log.info("sentry initialised for release %s@%s", SERVICE, VERSION)
    except Exception as exc:  # noqa: BLE001
        print(f"sentry unavailable: {exc}", file=sys.stderr)


# ── Tracing: Elastic APM and OpenSearch, simultaneously ──────────────────────
# Both, on purpose. elastic_apm and opensearch_apm are separate plugins that
# drifted apart at the 7.10 fork; a harness that exercises one and assumes the
# other holds is how API-shape bugs reach production.
def setup_tracing(app: FastAPI) -> None:
    otlp = os.getenv("OPENSEARCH_OTLP_ENDPOINT")
    if otlp:
        try:
            from opentelemetry import trace
            # gRPC, not HTTP. Data Prepper's `otel_trace_source` is gRPC-native on
            # 21890; posting OTLP/HTTP to it is accepted at the socket level and
            # then dropped, so the app logs "otlp tracing to ..." , Data Prepper
            # logs a healthy started pipeline, the indices get created — and the
            # document count stays at zero with nothing anywhere reporting an
            # error.
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            provider = TracerProvider(resource=Resource.create({
                "service.name": SERVICE,
                "service.version": VERSION,
                "deployment.environment": ENV,
                "host.name": HOSTNAME,
            }))
            # The gRPC exporter takes a host:port, without a scheme or a path.
            provider.add_span_processor(BatchSpanProcessor(
                OTLPSpanExporter(
                    endpoint=otlp.replace("http://", "").replace("https://", "").rstrip("/"),
                    insecure=True,
                )
            ))
            trace.set_tracer_provider(provider)
            FastAPIInstrumentor.instrument_app(app)
            log.info("otlp tracing to %s", otlp)
        except Exception as exc:  # noqa: BLE001
            print(f"otlp unavailable: {exc}", file=sys.stderr)

    apm = os.getenv("ELASTIC_APM_SERVER_URL")
    if apm:
        try:
            import elasticapm
            from elasticapm.contrib.starlette import ElasticAPM
            client = elasticapm.Client(
                server_url=apm,
                service_name=SERVICE,
                environment=ENV,
                service_version=VERSION,
                api_key=os.getenv("ELASTIC_APM_API_KEY") or None,
            )
            app.add_middleware(ElasticAPM, client=client)
            log.info("elastic apm to %s", apm)
        except Exception as exc:  # noqa: BLE001
            print(f"elastic apm unavailable: {exc}", file=sys.stderr)


# ── Dependency calls ─────────────────────────────────────────────────────────
class DependencyError(Exception):
    def __init__(self, dep: str, detail: str) -> None:
        super().__init__(f"{dep}: {detail}")
        self.dep = dep
        self.detail = detail


async def call_dependency(name: str, fn: Any) -> str:
    """Run a dependency check, record its metrics, and let failures propagate.

    `skipped` is a distinct outcome from `ok`. A checkout that returns 200 because
    its database was never configured proves nothing, and would make the
    storage-layer scenarios pass without the storage layer existing.
    """
    started = time.perf_counter()
    try:
        result = await fn()
    except Exception as exc:  # noqa: BLE001 — every failure is data here
        DEP_LAT.labels(SERVICE, name, "error").observe(time.perf_counter() - started)
        DEP_UP.labels(name).set(0)
        raise DependencyError(name, str(exc)[:200]) from exc
    DEP_LAT.labels(SERVICE, name, result).observe(time.perf_counter() - started)
    # NaN, not 0, for a dependency that is not configured. Prometheus reads NaN as
    # "no opinion" and 0 as "down", so setting 0 here would make every
    # not-yet-wired layer look like an outage — and an alert on
    # `checkout_dependency_up == 0` would fire for a dependency that was never
    # supposed to be there.
    DEP_UP.labels(name).set(1 if result == "ok" else float("nan"))
    return result


async def check_db() -> str:
    if not DEP_DB:
        return "skipped"
    import asyncpg
    conn = await asyncio.wait_for(asyncpg.connect(DEP_DB), timeout=DEP_TIMEOUT)
    try:
        await conn.fetchval("SELECT 1")
    finally:
        await conn.close()
    return "ok"


async def check_object_store(client: httpx.AsyncClient) -> str:
    if not DEP_OBJECT:
        return "skipped"
    r = await client.get(DEP_OBJECT, timeout=DEP_TIMEOUT)
    # RGW answers 200 on an anonymous list of the root; anything 5xx is the
    # storage layer failing, which is exactly what the Ceph scenarios inject.
    if r.status_code >= 500:
        raise RuntimeError(f"object store returned {r.status_code}")
    return "ok"


async def check_payments(client: httpx.AsyncClient) -> str:
    if not DEP_PAYMENTS:
        return "skipped"
    r = await client.get(f"{DEP_PAYMENTS.rstrip('/')}/health", timeout=DEP_TIMEOUT)
    if r.status_code >= 500:
        raise RuntimeError(f"payments returned {r.status_code}")
    return "ok"


# ── App ──────────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http = httpx.AsyncClient()
    log.info("%s %s starting on %s (env=%s)", SERVICE, VERSION, HOSTNAME, ENV)
    yield
    await app.state.http.aclose()


app = FastAPI(title=SERVICE, version=VERSION, lifespan=lifespan)


@app.middleware("http")
async def observe(request, call_next):
    started = time.perf_counter()
    path = request.url.path
    try:
        response = await call_next(request)
        status = response.status_code
    except Exception:
        REQS.labels(SERVICE, ENV, request.method, path, "500").inc()
        LAT.labels(SERVICE, ENV, path).observe(time.perf_counter() - started)
        raise
    REQS.labels(SERVICE, ENV, request.method, path, str(status)).inc()
    LAT.labels(SERVICE, ENV, path).observe(time.perf_counter() - started)
    return response


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/health")
async def health() -> JSONResponse:
    # Liveness only. It deliberately does NOT touch the dependencies: a health
    # check that fails when a downstream is down turns every dependency outage
    # into "the service is down", which is the single most common way a real
    # estate loses the ability to tell layers apart.
    return JSONResponse({"status": "ok", "service": SERVICE, "version": VERSION, "host": HOSTNAME})


@app.get("/ready")
async def ready() -> JSONResponse:
    results: dict[str, str] = {}
    for name, fn in (
        ("database", check_db),
        ("object_store", lambda: check_object_store(app.state.http)),
        ("payments", lambda: check_payments(app.state.http)),
    ):
        try:
            results[name] = await call_dependency(name, fn)
        except DependencyError as exc:
            results[name] = f"error: {exc.detail}"
    ok = all(v in ("ok", "skipped") for v in results.values())
    return JSONResponse({"ready": ok, "dependencies": results}, status_code=200 if ok else 503)


@app.get("/checkout")
@app.post("/checkout")
async def checkout() -> JSONResponse:
    """The business endpoint. One request, three layers, in order."""
    # Injected latency, before anything else, so it shows up as this service
    # being slow rather than as a dependency being slow.
    delay = float(fault("latency_seconds", "0") or 0)
    if delay:
        await asyncio.sleep(delay)

    # Injected error rate — the "the deploy itself is broken" scenario, where no
    # dependency is at fault and the correct answer is the application layer.
    rate = float(fault("error_rate", "0") or 0)
    if rate and random.random() < rate:
        log.error("checkout failed: injected application fault (release %s)", VERSION)
        return JSONResponse(
            {"error": "internal", "layer": "application", "version": VERSION}, status_code=500
        )

    steps: dict[str, str] = {}
    for name, fn in (
        ("database", check_db),
        ("object_store", lambda: check_object_store(app.state.http)),
        ("payments", lambda: check_payments(app.state.http)),
    ):
        try:
            steps[name] = await call_dependency(name, fn)
        except DependencyError as exc:
            # Logged at ERROR with the dependency named, because this is the line
            # the graylog and APM plugins are expected to surface. The HTTP status
            # is deliberately the same 500 whichever layer failed — the layer is
            # in the evidence, not in the status code.
            log.error(
                "checkout failed at %s: %s", exc.dep, exc.detail,
                extra={"dependency": exc.dep, "layer": exc.dep},
            )
            return JSONResponse(
                {"error": "dependency_failed", "dependency": exc.dep, "detail": exc.detail},
                status_code=500,
            )

    return JSONResponse({"status": "ok", "order_id": f"ord-{int(time.time()*1000)}", "steps": steps})


if __name__ == "__main__":
    setup_logging()
    setup_sentry()
    setup_tracing(app)
    uvicorn.run(
        app,
        host="0.0.0.0",  # noqa: S104 — lab instance on a private bridge
        port=int(os.getenv("PORT", "8080")),
        log_config=None,  # our JSON formatter, not uvicorn's
        access_log=False,  # the middleware records what matters
    )
