#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m236
gofmt -w hat/hatSql/m236_on_demand_refresh.go hat/hatSql/m236_on_demand_refresh_test.go hat/hatSql/m236_on_demand_refresh_benchmark_test.go
