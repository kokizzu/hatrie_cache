#!/usr/bin/env bash
set -euo pipefail

expected=(
	BENCHMARK.md
	CH051_LOW_CARDINALITY.md
	INSPIRATION_BACKLOG.md
	Makefile
	README.md
	hat/hatDataStructure/low_cardinality.go
	hat/hatDataStructure/low_cardinality_benchmark_test.go
	hat/hatDataStructure/low_cardinality_test.go
	scripts/benchmark-ch051-c203.sh
	scripts/commit-ch051-c203.sh
	scripts/format-ch051-c203.sh
	scripts/inspect-staged-ch051-c203.sh
	scripts/push-ch051-c203.sh
	scripts/stage-ch051-c203.sh
	scripts/test-ch051-c203.sh
	scripts/verify-ch051-c203.sh
)
mapfile -t staged < <(git diff --cached --name-only)
if ((${#staged[@]} != ${#expected[@]})); then
	printf 'unexpected staged file count: got %d, want %d\n' "${#staged[@]}" "${#expected[@]}" >&2
	printf '%s\n' "${staged[@]}" >&2
	exit 1
fi
for index in "${!expected[@]}"; do
	if [[ "${staged[index]}" != "${expected[index]}" ]]; then
		printf 'unexpected staged path at index %d: got %s, want %s\n' "$index" "${staged[index]}" "${expected[index]}" >&2
		exit 1
	fi
done
git commit -m 'CH-051 add low-cardinality string columns'
