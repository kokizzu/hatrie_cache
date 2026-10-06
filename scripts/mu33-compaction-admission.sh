#!/usr/bin/env bash
set -euo pipefail

mode="${1:-test}"
pipeline="./hat/hatPipeline"
storage="./hat/hatStorage"
go_test=(go test -tags mu33local)
go_vet=(go vet -tags mu33local)

case "$mode" in
  format)
    gofmt -w \
      hat/hatPipeline/frontier_retention.go \
      hat/hatPipeline/mu033_frontier_compaction_admission_test.go \
      hat/hatStorage/compaction_control.go \
      hat/hatStorage/mu033_compaction_admission.go \
      hat/hatStorage/mu033_compaction_admission_test.go \
      hat/hatStorage/mu033_compaction_admission_benchmark_test.go \
      hat/hatStorage/mu033_compaction_admission_baseline_benchmark_test.go
    ;;
  baseline)
    "${go_test[@]}" "$storage" -run '^$' -bench '^BenchmarkMU33Baseline' -benchmem -count=5
    ;;
  test)
    "${go_test[@]}" "$pipeline" "$storage" -run 'TestMU33' -count=1
    ;;
  package)
    "${go_test[@]}" "$pipeline" "$storage" -count=1
    ;;
  benchmark)
    "${go_test[@]}" "$storage" -run '^$' -bench '^BenchmarkMU33' -benchmem -count=5
    ;;
  race)
    "${go_test[@]}" -race "$pipeline" "$storage" -run 'TestMU33' -count=1
    ;;
  vet)
    "${go_vet[@]}" "$pipeline" "$storage"
    ;;
  diff-check)
    git diff --check
    ;;
  status)
    git status --short
    ;;
  stage)
    git add \
      BENCHMARK.md \
      Makefile \
      MU033_AS_OF_COMPACTION_ADMISSION.md \
      PRODUCT_IDEA_GAPS.md \
      README.md \
      hat/hatPipeline/frontier_retention.go \
      hat/hatPipeline/mu033_frontier_compaction_admission_test.go \
      hat/hatStorage/compaction_control.go \
      hat/hatStorage/mu033_compaction_admission.go \
      hat/hatStorage/mu033_compaction_admission_test.go \
      hat/hatStorage/mu033_compaction_admission_benchmark_test.go \
      hat/hatStorage/mu033_compaction_admission_baseline_benchmark_test.go \
      scripts/mu33-compaction-admission.sh
    ;;
  commit)
    git commit -m 'feat: admit storage compaction behind as-of retention [skip ci]'
    ;;
  push)
    git push -u origin HEAD
    ;;
  *)
    printf 'usage: %s {format|baseline|test|package|benchmark|race|vet|diff-check|status|stage|commit|push}\n' "$0" >&2
    exit 2
    ;;
esac
