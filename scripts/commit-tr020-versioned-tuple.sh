#!/bin/sh
set -eu
git add BENCHMARK.md ENGINE_IDEAS.md INSPIRATION_BACKLOG.md README.md TR020_VERSIONED_TUPLE.md hat/hatDataStructure/tuple_format.go hat/hatDataStructure/tr020_versioned_tuple.go hat/hatDataStructure/tr020_versioned_tuple_test.go hat/hatDataStructure/tr020_versioned_tuple_benchmark_test.go scripts/benchmark-tr020-baseline.sh scripts/benchmark-tr020-versioned-tuple.sh scripts/commit-tr020-versioned-tuple.sh scripts/format-tr020-versioned-tuple.sh scripts/push-tr020-versioned-tuple.sh scripts/race-tr020-versioned-tuple.sh scripts/review-tr020-versioned-tuple.sh scripts/stage-tr020-versioned-tuple.sh scripts/test-tr020-package.sh scripts/test-tr020-versioned-tuple.sh scripts/verify-tr020-versioned-tuple-docs.sh scripts/vet-tr020-versioned-tuple.sh Makefile
git diff --cached --check
git commit -m 'Add versioned tuple schema boundaries [skip ci]'
