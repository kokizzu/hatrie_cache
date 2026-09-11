#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/ch049_external_dictionary_baseline_benchmark_test.go hat/hatSql/external_dictionary.go hat/hatSql/external_dictionary_test.go hat/hatSql/external_dictionary_benchmark_test.go
