#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatAuth -run '^$' -bench '^(BenchmarkMU021BeforePolicyAuthorize|BenchmarkMU021AfterRoleCatalogAuthorize|BenchmarkTU33)' -benchmem -count=5
