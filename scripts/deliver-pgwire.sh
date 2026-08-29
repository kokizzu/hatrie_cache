#!/bin/sh
set -eu

git diff --check -- Makefile README.md PGWIRE.md hat/hatPgWire/server.go hat/hatPgWire/server_test.go hat/hatSql/pgwire.go hat/hatSql/pgwire_test.go scripts/format-pgwire.sh scripts/test-pgwire-server.sh scripts/test-pgwire-sql-adapter.sh scripts/deliver-pgwire.sh
git add -- Makefile README.md PGWIRE.md hat/hatPgWire/server.go hat/hatPgWire/server_test.go hat/hatSql/pgwire.go hat/hatSql/pgwire_test.go scripts/format-pgwire.sh scripts/test-pgwire-server.sh scripts/test-pgwire-sql-adapter.sh scripts/deliver-pgwire.sh
git commit -m "feat(sql): add PostgreSQL wire query server"
git push
