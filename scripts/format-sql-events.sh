#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/events.go ./hat/hatSql/events_test.go
