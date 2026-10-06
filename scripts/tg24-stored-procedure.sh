#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
package="./hat/hatSql"
test_pattern='TestTU03'
benchmark_pattern='^BenchmarkTU03'
go_test=(go test)
go_vet=(go vet)

case "$mode" in
  format)
    gofmt -w \
      hat/hatSql/tu03_stored_procedure_registry.go \
      hat/hatSql/tu03_stored_procedure_registry_test.go \
      hat/hatSql/tu03_stored_procedure_baseline_benchmark_test.go \
      hat/hatSql/tu03_stored_procedure_benchmark_test.go
    ;;
  baseline)
    "${go_test[@]}" "$package" -run '^$' -bench '^BenchmarkTU03BaselineVersionedFunction(Resolve|Invoke)$' -benchmem -count=5
    ;;
  test)
    "${go_test[@]}" "$package" -run "$test_pattern" -count=1
    ;;
  package)
    "${go_test[@]}" "$package" -count=1
    ;;
  benchmark)
    "${go_test[@]}" "$package" -run '^$' -bench "$benchmark_pattern" -benchmem -count=5
    ;;
  race)
    "${go_test[@]}" -race "$package" -run "$test_pattern" -count=1
    ;;
  vet)
    "${go_vet[@]}" "$package"
    ;;
  diff-check)
    git diff --check
    ;;
  status)
    git status --short
    ;;
  stage)
    git add \
      Makefile \
      BENCHMARK.md \
      PRODUCT_IDEA_GAPS.md \
      README.md \
      TU03_STORED_PROCEDURE_REGISTRY.md \
      hat/hatSql/tu03_stored_procedure_registry.go \
      hat/hatSql/tu03_stored_procedure_registry_test.go \
      hat/hatSql/tu03_stored_procedure_baseline_benchmark_test.go \
      hat/hatSql/tu03_stored_procedure_benchmark_test.go \
      scripts/tg24-stored-procedure.sh
    ;;
  commit)
    git commit -m 'feat: add trusted stored procedure registry [skip ci]'
    ;;
  push)
    git push -u origin HEAD
    ;;
  *)
    printf 'usage: %s {format|baseline|test|package|benchmark|race|vet|diff-check|status|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
