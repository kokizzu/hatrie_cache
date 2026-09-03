#!/bin/sh
set -eu

gofmt -w hat/hatSql/contracts.go hat/hatSql/keyset.go hat/hatSql/keyset_pagination_test.go
gofmt -w hat/hatCache/sql_query.go hat/hatCache/sql_keyset_pagination_test.go
gofmt -w hat/hatCache/sql_keyset_monitoring_test.go
gofmt -w hat/hatSql/model.go hat/hatCache/monitoring.go
gofmt -w hat/hatSql/client.go hat/hatSql/keyset_client_test.go
gofmt -w hat/hatCache/sql_keyset_pagination_benchmark_test.go
