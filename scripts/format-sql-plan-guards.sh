#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/plan_guard.go hat/hatSql/plan_guard_test.go
