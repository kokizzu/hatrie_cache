#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSpill/quota.go hat/hatSpill/quota_test.go hat/hatSpill/quota_benchmark_test.go
