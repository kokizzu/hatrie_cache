#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/mu027_logical_publication.go hat/hatSql/mu027_logical_publication_test.go hat/hatSql/mu027_logical_publication_benchmark_test.go
