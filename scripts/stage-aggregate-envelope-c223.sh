#!/usr/bin/env bash
set -euo pipefail

root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
staged_makefile="$(mktemp "${TMPDIR:-/tmp}/hatrie-aggregate-envelope-c223-makefile.XXXXXX")"
trap 'rm -f "$staged_makefile"' EXIT

git -C "$root" show HEAD:Makefile > "$staged_makefile"
printf '%s\n' '' \
	'.PHONY: test-aggregate-envelope-c223' \
	'test-aggregate-envelope-c223:' \
	$'\t@bash ./scripts/run-aggregate-envelope-c223.sh test' '' \
	'.PHONY: format-aggregate-envelope-c223' \
	'format-aggregate-envelope-c223:' \
	$'\t@bash ./scripts/format-aggregate-envelope-c223.sh' '' \
	'.PHONY: test-aggregate-envelope-package-c223' \
	'test-aggregate-envelope-package-c223:' \
	$'\t@bash ./scripts/run-aggregate-envelope-c223.sh package' '' \
	'.PHONY: race-aggregate-envelope-c223' \
	'race-aggregate-envelope-c223:' \
	$'\t@bash ./scripts/run-aggregate-envelope-c223.sh race' '' \
	'.PHONY: vet-aggregate-envelope-c223' \
	'vet-aggregate-envelope-c223:' \
	$'\t@bash ./scripts/run-aggregate-envelope-c223.sh vet' '' \
	'.PHONY: benchmark-aggregate-envelope-c223' \
	'benchmark-aggregate-envelope-c223:' \
	$'\t@bash ./scripts/run-aggregate-envelope-c223.sh benchmark' '' \
	'.PHONY: check-aggregate-envelope-c223' \
	'check-aggregate-envelope-c223:' \
	$'\t@bash ./scripts/check-aggregate-envelope-c223.sh' '' \
	'.PHONY: stage-aggregate-envelope-c223' \
	'stage-aggregate-envelope-c223:' \
	$'\t@bash ./scripts/stage-aggregate-envelope-c223.sh' '' \
	'.PHONY: commit-aggregate-envelope-c223' \
	'commit-aggregate-envelope-c223:' \
	$'\t@bash ./scripts/commit-aggregate-envelope-c223.sh' '' \
	'.PHONY: push-aggregate-envelope-c223' \
	'push-aggregate-envelope-c223:' \
	$'\t@bash ./scripts/push-aggregate-envelope-c223.sh' >> "$staged_makefile"
blob="$(git -C "$root" hash-object -w "$staged_makefile")"
git -C "$root" update-index --add --cacheinfo 100644 "$blob" Makefile
git -C "$root" add -- \
	INSPIRATION_ROUND2.md \
	PRODUCT_IDEA_GAPS.md \
	ADOPTED_QUERY_ENGINE_IDEAS.md \
	BENCHMARK.md \
	hat/hatDataStructure/partial_aggregate_envelope.go \
	hat/hatDataStructure/partial_aggregate_state.go \
	hat/hatDataStructure/partial_aggregate_envelope_test.go \
	hat/hatDataStructure/partial_aggregate_envelope_benchmark_test.go \
	hat/hatCache/partial_aggregate_state.go \
	hat/hatCache/partial_aggregate_envelope_test.go \
	hat/hatCache/partial_aggregate_envelope_benchmark_test.go \
	scripts/run-aggregate-envelope-c223.sh \
	scripts/format-aggregate-envelope-c223.sh \
	scripts/check-aggregate-envelope-c223.sh \
	scripts/stage-aggregate-envelope-c223.sh \
	scripts/commit-aggregate-envelope-c223.sh \
	scripts/push-aggregate-envelope-c223.sh
printf '%s\n' 'stage-aggregate-envelope-c223: staged explicit feature paths'
