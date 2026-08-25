# SQL Compatibility Suite

`sql_compatibility_test.go` contains shared supported-query cases and compares
Hatrie SQL result rows with SQLite, PostgreSQL, and MariaDB. It intentionally covers the
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

The PostgreSQL test skips when the URL or `psql` is unavailable.

Run the MariaDB cases with a database name and an optional client defaults
file containing the host, user, TLS, and password settings:

```sh
HATRIE_MARIADB_DATABASE=hatrie_compat \
HATRIE_MARIADB_DEFAULTS_FILE=/run/secrets/hatrie-mariadb.cnf \
make run CMD='go test . -run TestSQLCompatibilityMariaDB'
```

The MariaDB test skips when `HATRIE_MARIADB_DATABASE` or the `mariadb` client
is unavailable. The defaults file is passed directly to the client and is not
logged by the test. Both reference databases require no schema because every
case uses inline values. Add a case only when Hatrie accepts the corresponding
query and its documented semantics match all reference engines.
