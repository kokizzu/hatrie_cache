#!/usr/bin/env sh
set -eu

gofmt -w hat/hatSql/partial_aggregate_state.go hat/hatSql/partial_aggregate_state_test.go hat/hatSql/partial_aggregate_state_public_test.go
