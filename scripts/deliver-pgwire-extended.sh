#!/bin/sh
set -eu

git diff --check -- Makefile PGWIRE.md hat/hatPgWire/server.go hat/hatPgWire/server_test.go hat/hatSql/pgwire.go hat/hatSql/pgwire_test.go scripts/check-pgwire-client-tools.sh scripts/deliver-pgwire-extended.sh
git add -- Makefile PGWIRE.md hat/hatPgWire/server.go hat/hatPgWire/server_test.go hat/hatSql/pgwire.go hat/hatSql/pgwire_test.go scripts/check-pgwire-client-tools.sh scripts/deliver-pgwire-extended.sh
git commit -m "feat(pgwire): bound portal state"
git push
