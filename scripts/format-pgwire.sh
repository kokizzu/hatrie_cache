#!/bin/sh
set -eu

gofmt -w hat/hatPgWire/server.go hat/hatPgWire/server_test.go hat/hatSql/pgwire.go hat/hatSql/pgwire_test.go
