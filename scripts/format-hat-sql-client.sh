#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/client.go ./hat/hatSql/client_test.go
