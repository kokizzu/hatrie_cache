#!/usr/bin/env bash
set -eu

gofmt -w \
  hat/hatSql/ch043_asof_join.go \
  hat/hatSql/ch043_asof_join_test.go \
  hat/hatSql/temporal_analytics.go
