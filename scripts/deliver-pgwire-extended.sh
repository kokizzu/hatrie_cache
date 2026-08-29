#!/bin/sh
set -eu

git diff --check -- Makefile PGWIRE.md hat/hatPgWire/cancel.go hat/hatPgWire/cancel_test.go hat/hatPgWire/server.go hat/hatPgWire/server_test.go hat/hatSql/pgwire.go hat/hatSql/pgwire_test.go scripts/check-pgwire-client-tools.sh scripts/deliver-pgwire-extended.sh scripts/format-pgwire.sh scripts/inspect-pgwire-protocol.sh
git add -- Makefile PGWIRE.md hat/hatPgWire/cancel.go hat/hatPgWire/cancel_test.go hat/hatPgWire/server.go hat/hatPgWire/server_test.go hat/hatSql/pgwire.go hat/hatSql/pgwire_test.go scripts/check-pgwire-client-tools.sh scripts/deliver-pgwire-extended.sh scripts/format-pgwire.sh scripts/inspect-pgwire-protocol.sh
git commit -m "fix(pgwire): recover extended query errors at sync"
git push
