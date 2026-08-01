# checkout-api

The application under test in the rooca e2e estate. One service, observed by
every integration at once: metrics to Prometheus and Datadog, logs to Graylog,
traces to Elastic APM and OpenSearch, errors to Sentry, host state to Zabbix and
the rooca agent.

It runs on a Proxmox-hosted database, a Ceph object store and a Kubernetes
payments service, so a fault in any layer surfaces here as the same 5xx.
