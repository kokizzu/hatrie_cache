#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatSql/cdc_envelope.go \
	hat/hatSql/cdc_envelope_test.go \
	hat/hatSql/cdc_envelope_benchmark_test.go
