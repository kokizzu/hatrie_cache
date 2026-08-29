#!/usr/bin/env sh
set -eu

printf '%s\n' '== pgwire extended error handling =='
sed -n '80,350p' hat/hatPgWire/server.go
grep -n -A35 -B8 'writeErrorAndReady\|writeError' hat/hatPgWire/server.go
grep -n -A8 -B8 'readReadyForQuery' hat/hatPgWire/server_test.go
