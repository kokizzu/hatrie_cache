#!/usr/bin/env bash
set -euo pipefail
git add -- \
	BENCHMARK.md \
	INSPIRATION_ROUND2.md \
	README.md \
	T228_TUPLE_FORMAT_COMPATIBILITY.md \
	TUPLE_FORMAT_NEGOTIATION.md \
	hat/hatDataStructure/tuple_format_reader.go \
	hat/hatDataStructure/t228_tuple_reader_baseline_test.go \
	hat/hatDataStructure/t228_tuple_format_compatibility_test.go \
	Makefile \
	scripts/benchmark-t228-before.sh \
	scripts/benchmark-t228.sh \
	scripts/format-t228.sh \
	scripts/commit-t228.sh \
	scripts/push-t228.sh \
	scripts/race-t228.sh \
	scripts/review-t228.sh \
	scripts/stage-t228.sh \
	scripts/test-t228.sh \
	scripts/test-t228-package.sh \
	scripts/verify-t228.sh \
	scripts/vet-t228.sh
