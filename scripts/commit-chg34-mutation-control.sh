#!/usr/bin/env bash
set -euo pipefail

paths=(
  Makefile
  CHG34_MUTATION_CONTROL.md
  hat/hatSql/mutation_controller.go
  hat/hatSql/mutation_controller_test.go
  hat/hatSql/mutation_controller_benchmark_test.go
  scripts/bench-chg34-mutation-control.sh
  scripts/format-chg34-mutation-control.sh
  scripts/race-chg34-mutation-control.sh
  scripts/test-chg34-mutation-control-package.sh
  scripts/test-chg34-mutation-control.sh
  scripts/vet-chg34-mutation-control.sh
  scripts/commit-chg34-mutation-control.sh
  scripts/push-chg34-mutation-control.sh
)

git add -- "${paths[@]}"
git diff --cached --check
git commit --only -m "feat(sql): add mutation priority control" -- "${paths[@]}"
