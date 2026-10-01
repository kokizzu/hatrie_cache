#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
test)
  go test ./hat/hatAuth -run '^TestTU03StoredFunctionRegistry' -count=1
  ;;
package)
  go test ./hat/hatAuth -count=1
  ;;
race)
  go test -race ./hat/hatAuth -run '^TestTU03StoredFunctionRegistry' -count=1
  ;;
vet)
  go vet ./hat/hatAuth
  ;;
verify)
  test -f TU003_STORED_FUNCTION_REGISTRY.md
  rg -q 'TU003_STORED_FUNCTION_REGISTRY.md' README.md PRODUCT_IDEA_GAPS.md BENCHMARK.md
  rg -q 'T-U03 Stored Function Registry' BENCHMARK.md
  git diff --check
  ;;
status)
  git status --short
  git diff --stat
  ;;
format)
  gofmt -w hat/hatAuth/stored_function_registry.go hat/hatAuth/t_u03_stored_function_registry_test.go hat/hatAuth/t_u03_stored_function_registry_benchmark_test.go
  ;;
benchmark)
  go test ./hat/hatAuth -run '^$' -bench '^BenchmarkTU03' -benchmem -count=5
  ;;
*)
  printf 'usage: %s test|package|race|vet|verify|status|format|benchmark\n' "$0" >&2
  exit 2
  ;;
esac
