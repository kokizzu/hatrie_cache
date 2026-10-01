#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
case "$mode" in
test)
  go test ./hat/hatAuth -run '^TestTU33RoleCatalogStoredFunctionAuthorizer' -count=1
  ;;
package)
  go test ./hat/hatAuth -count=1
  ;;
race)
  go test -race ./hat/hatAuth -run '^TestTU33RoleCatalogStoredFunctionAuthorizer' -count=1
  ;;
vet)
  go vet ./hat/hatAuth
  ;;
verify)
  test -f TU033_ROLE_FUNCTION_GRANTS.md
  rg -q 'TU033_ROLE_FUNCTION_GRANTS.md' README.md PRODUCT_IDEA_GAPS.md
  rg -q 'T-U33 Role-Based Function Grants' BENCHMARK.md
  git diff --check
  ;;
status)
  git status --short
  git diff --stat
  ;;
format)
  gofmt -w hat/hatAuth/role_catalog_stored_function_authorizer.go hat/hatAuth/t_u33_role_function_authorizer_test.go hat/hatAuth/t_u33_role_function_authorizer_benchmark_test.go
  ;;
benchmark)
  go test ./hat/hatAuth -run '^$' -bench '^BenchmarkTU33' -benchmem -count=5
  ;;
*)
  printf 'usage: %s test|package|race|vet|verify|status|format|benchmark\n' "$0" >&2
  exit 2
  ;;
esac
