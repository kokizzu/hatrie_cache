# OpenAPI Management API

Every monitoring server serves an OpenAPI 3.1 document at `GET /openapi.json`.
It describes the stable management operations for health, entries, SQL queries,
commands, and Grafana datasource requests, including bearer authentication and
the SQL request schema.

Download it from the running server and generate language bindings with the
tooling chosen by the consuming project:

```sh
curl -fsS http://127.0.0.1:8080/openapi.json -o hatrie-monitoring.openapi.json
openapi-generator-cli generate -i hatrie-monitoring.openapi.json -g typescript-fetch -o generated/hatrie-monitoring
```

The public Go `hatMonitoring.Client` provides hand-maintained typed bindings
for the health and entries operations. Generated clients must pass the operator
token as an `Authorization: Bearer` header when monitoring authentication is
enabled.
