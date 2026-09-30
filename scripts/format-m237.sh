#!/bin/sh
set -eu

cd /tmp/hatrie-cache-m237
gofmt -w hat/hatSql/m237_lazy_hydration.go hat/hatSql/m237_lazy_hydration_test.go hat/hatSql/m237_lazy_hydration_benchmark_test.go
