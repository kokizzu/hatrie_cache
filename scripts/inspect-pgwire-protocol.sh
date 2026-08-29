#!/usr/bin/env sh
set -eu

printf '%s\n' '== pgwire cancellation registry and startup protocol =='
sed -n '1,220p' hat/hatPgWire/cancel.go
sed -n '1,190p' hat/hatPgWire/server.go
sed -n '650,770p' hat/hatPgWire/server.go
sed -n '1,210p' hat/hatPgWire/cancel_test.go
