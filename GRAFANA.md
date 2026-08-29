# Grafana Datasource API

The monitoring server exposes Grafana SimpleJson-compatible endpoints for a
small SQL datasource adapter:

- `POST /api/grafana/search` returns declared `SQLCatalog.Namespaces`; an
  optional JSON `target` prefix filters the list.
- `POST /api/grafana/query` accepts `{"targets":[{"refId":"A","target":"..."}]}`
  and returns Grafana table results with `columns` and `rows`.

Each target is executed by the Hatrie SQL engine using the same request-size
limit, SQL rate limiter, authentication, RBAC policy, diagnostics, and audit
records as `POST /api/sql`. Queries use the Hatrie SQL dialect documented in
[SQL.md](SQL.md). Configure the datasource URL with the monitoring base URL
and call these paths from a SimpleJson-compatible Grafana datasource plugin.

The adapter returns table data. Native Grafana time-series frames and alerting
semantics remain a follow-on integration.
