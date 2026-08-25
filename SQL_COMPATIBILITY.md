# SQL Compatibility Suite

`sql_compatibility_test.go` contains shared supported-query cases and compares
Hatrie SQL result rows with SQLite and PostgreSQL. It intentionally covers the
portable subset rather than claiming full dialect compatibility.

Run the SQLite cases:

```sh
make run CMD='go test . -run TestSQLCompatibilitySQLite'
```

Run the PostgreSQL cases against an isolated database:

```sh
HATRIE_POSTGRES_URL='postgres://user:password@host/database?sslmode=require' \
  make run CMD='go test . -run TestSQLCompatibilityPostgres'
```

The PostgreSQL test skips when the URL or `psql` is unavailable. A configured
test database must contain no required schema because every case uses `VALUES`
CTEs only. Add a case only when Hatrie accepts the corresponding query and its
documented semantics match both reference engines.
