#!/usr/bin/env sh
set -eu

gofmt -w ./hat/hatSql/temporal_analytics.go ./hat/hatSql/temporal_analytics_test.go
