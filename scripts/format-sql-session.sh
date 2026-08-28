#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/session.go ./hat/hatSql/session_test.go
