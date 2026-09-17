#!/usr/bin/env bash
set -euo pipefail

gofmt -w \
	hat/hatAuth/mu021_role_catalog_baseline_benchmark_test.go \
	hat/hatAuth/mu021_role_catalog_test.go \
	hat/hatAuth/role_catalog.go
