#!/bin/sh
set -eu

git diff --check -- Makefile hat/hatPgWire/server.go hat/hatPgWire/server_test.go scripts/deliver-pgwire-extended.sh
git add -- Makefile hat/hatPgWire/server.go hat/hatPgWire/server_test.go scripts/deliver-pgwire-extended.sh
git commit -m "feat(pgwire): support extended prepared query flow"
git push
