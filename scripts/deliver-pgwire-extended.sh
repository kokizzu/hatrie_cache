#!/bin/sh
set -eu

git diff --check -- PGWIRE.md hat/hatPgWire/server.go hat/hatPgWire/server_test.go hat/hatSql/pgwire.go hat/hatSql/pgwire_test.go scripts/deliver-pgwire-extended.sh
git add -- PGWIRE.md hat/hatPgWire/server.go hat/hatPgWire/server_test.go hat/hatSql/pgwire.go hat/hatSql/pgwire_test.go scripts/deliver-pgwire-extended.sh
git commit -m "feat(pgwire): describe bound portal result schemas"
git push
