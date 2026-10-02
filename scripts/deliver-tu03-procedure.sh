#!/usr/bin/env bash
set -euo pipefail

mode=${1:-status}
case "$mode" in
  status)
    git status --short --untracked-files=all
    ;;
  stage)
    git add Makefile README.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md PRODUCT_IDEA_GAPS.md TU03_STORED_PROCEDURE_REGISTRY.md \
      hat/hatProcedure/registry.go hat/hatProcedure/registry_test.go hat/hatProcedure/registry_benchmark_test.go \
      scripts/test-tu03-procedure.sh scripts/test-tu03-package.sh scripts/format-tu03-procedure.sh \
      scripts/race-tu03-procedure.sh scripts/vet-tu03-procedure.sh scripts/verify-tu03-procedure.sh \
      scripts/benchmark-tu03-procedure.sh scripts/deliver-tu03-procedure.sh
    git diff --cached --name-status
    ;;
  commit)
    git commit -m 'feat(procedure): add bounded stored procedure registry [skip ci]'
    ;;
  push)
    git push -u origin codex/t-u02-peer-daemon
    ;;
  *)
    printf 'unknown delivery mode: %s\n' "$mode" >&2
    exit 2
    ;;
esac
