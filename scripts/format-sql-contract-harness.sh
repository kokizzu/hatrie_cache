#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/contract_harness.go ./hat/hatSql/contract_harness_test.go
