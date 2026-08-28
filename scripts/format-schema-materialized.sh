#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSchema/materialized.go ./hat/hatSchema/materialized_test.go ./hat/hatSql/session.go ./hat/hatSql/session_test.go
