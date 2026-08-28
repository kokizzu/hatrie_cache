#!/usr/bin/env sh
set -eu

git diff --check -- Makefile hat/hatSql/mutation_workload_test.go hat/hatCache/seeded_workload_test.go scripts/test-seeded-mutation-workloads.sh scripts/format-seeded-mutation-workloads.sh scripts/review-seeded-mutation-workloads.sh scripts/commit-seeded-mutation-workloads.sh
git add Makefile hat/hatSql/mutation_workload_test.go hat/hatCache/seeded_workload_test.go scripts/test-seeded-mutation-workloads.sh scripts/format-seeded-mutation-workloads.sh scripts/review-seeded-mutation-workloads.sh scripts/commit-seeded-mutation-workloads.sh
git diff --cached --check
git commit -m 'test: add seeded mutation and recovery workloads'
git push
