#!/usr/bin/env bash
set -euo pipefail

isolated_index="$(mktemp /tmp/.mz021-index.XXXXXX)"
makefile_tmp="$(mktemp /tmp/.mz021-makefile.XXXXXX)"
ideas_tmp="$(mktemp /tmp/.mz021-ideas.XXXXXX)"
benchmark_tmp="$(mktemp /tmp/.mz021-benchmark.XXXXXX)"
trap 'rm -f -- "$isolated_index" "$makefile_tmp" "$ideas_tmp" "$benchmark_tmp"' EXIT
rm -f -- "$isolated_index"
GIT_INDEX_FILE="$isolated_index" git read-tree HEAD

git show HEAD:Makefile > "$makefile_tmp"
if ! grep -q '^test-mz021-replica-hot-handoff:' "$makefile_tmp"; then
	printf '\n%s\n' \
		'.PHONY: test-mz021-replica-hot-handoff format-mz021-replica-hot-handoff benchmark-mz021-replica-hot-handoff test-mz021-package race-mz021-replica-hot-handoff vet-mz021-replica-hot-handoff verify-mz021-docs commit-mz021-replica-hot-handoff push-mz021-replica-hot-handoff' \
		'test-mz021-replica-hot-handoff:' \
		$'\tbash ./scripts/test-mz021-replica-hot-handoff.sh' \
		'format-mz021-replica-hot-handoff:' \
		$'\tbash ./scripts/format-mz021-replica-hot-handoff.sh' \
		'benchmark-mz021-replica-hot-handoff:' \
		$'\tbash ./scripts/benchmark-mz021-replica-hot-handoff.sh' \
		'test-mz021-package:' \
		$'\tbash ./scripts/test-mz021-package.sh' \
		'race-mz021-replica-hot-handoff:' \
		$'\tbash ./scripts/race-mz021-replica-hot-handoff.sh' \
		'vet-mz021-replica-hot-handoff:' \
		$'\tbash ./scripts/vet-mz021-replica-hot-handoff.sh' \
		'verify-mz021-docs:' \
		$'\tbash ./scripts/verify-mz021-docs.sh' \
		'commit-mz021-replica-hot-handoff:' \
		$'\tbash ./scripts/commit-mz021-replica-hot-handoff.sh' \
		'push-mz021-replica-hot-handoff:' \
		$'\tbash ./scripts/push-mz021-replica-hot-handoff.sh' >> "$makefile_tmp"
fi

ideas_line='| MZ-021 | Replica hot handoff | Partially adopted as importable `hatSql.ReplicaHotHandoff`: a bounded snapshot install, contiguous query-state delta catch-up, target readiness check, and generation/fencing-bound promotion token avoid a cold rebuild; transport, persistence, source fencing, and route publication remain caller-owned. See [MZ021_REPLICA_HOT_HANDOFF.md](MZ021_REPLICA_HOT_HANDOFF.md). | High |'
git show HEAD:ENGINE_IDEAS.md > "$ideas_tmp"
if grep -q '^| MZ-021 | Replica hot handoff |' "$ideas_tmp"; then
	awk -v replacement="$ideas_line" 'index($0, "| MZ-021 | Replica hot handoff |") == 1 { print replacement; next } { print }' "$ideas_tmp" > "$ideas_tmp.new"
	mv -- "$ideas_tmp.new" "$ideas_tmp"
else
	printf '%s\n' "$ideas_line" >> "$ideas_tmp"
fi

git show HEAD:BENCHMARK.md > "$benchmark_tmp"
if ! grep -q '^### MZ-021 Replica Hot Handoff$' "$benchmark_tmp"; then
	printf '%s\n' \
		'' \
		'### MZ-021 Replica Hot Handoff' \
		'' \
		'Command: `make benchmark-mz021-replica-hot-handoff` (five samples per case,' \
		'AMD Ryzen 9 5950X, Linux/amd64). The warm-tail case includes a 64 KiB' \
		'snapshot, validation, one delta, readiness, and lifecycle accounting. The' \
		'direct control only calls the target'"'"'s delta callback, so it is a lower-bound' \
		'control rather than an equivalent replica implementation.' \
		'' \
		'| Case | Raw ns/op samples | B/op | allocs/op |' \
		'| --- | --- | ---: | ---: |' \
		'| Warm tail before redundant-copy removal | 22304, 19934, 18943, 20813, 20444 | 131648 | 5 |' \
		'| Warm tail after redundant-copy removal | 11196, 10090, 9024, 8506, 8167 | 66112 | 4 |' \
		'| Direct tail callback control | 1.798, 1.796, 1.720, 1.669, 1.785 | 0 | 0 |' \
		'' \
		'The warm-tail median improved from 20,444 to 9,024 ns/op (**2.27x faster**),' \
		'retained benchmark allocation fell from 131,648 to 66,112 B/op (**1.99x' \
		'lower**), and allocations fell from 5 to 4 (**1.25x fewer**). The direct' \
		'control is not a user-visible speed claim; it excludes snapshot transfer,' \
		'validation, readiness, and promotion safety checks.' >> "$benchmark_tmp"
fi

makefile_blob="$(git hash-object -w "$makefile_tmp")"
ideas_blob="$(git hash-object -w "$ideas_tmp")"
benchmark_blob="$(git hash-object -w "$benchmark_tmp")"
GIT_INDEX_FILE="$isolated_index" git update-index --add --cacheinfo 100644,"$makefile_blob",Makefile
GIT_INDEX_FILE="$isolated_index" git update-index --add --cacheinfo 100644,"$ideas_blob",ENGINE_IDEAS.md
GIT_INDEX_FILE="$isolated_index" git update-index --add --cacheinfo 100644,"$benchmark_blob",BENCHMARK.md
GIT_INDEX_FILE="$isolated_index" git add -- \
	MZ021_REPLICA_HOT_HANDOFF.md \
	hat/hatSql/mz021_replica_hot_handoff.go \
	hat/hatSql/mz021_replica_hot_handoff_test.go \
	hat/hatSql/mz021_replica_hot_handoff_benchmark_test.go \
	scripts/benchmark-mz021-replica-hot-handoff.sh \
	scripts/commit-mz021-replica-hot-handoff.sh \
	scripts/format-mz021-replica-hot-handoff.sh \
	scripts/push-mz021-replica-hot-handoff.sh \
	scripts/race-mz021-replica-hot-handoff.sh \
	scripts/test-mz021-package.sh \
	scripts/test-mz021-replica-hot-handoff.sh \
	scripts/verify-mz021-docs.sh \
	scripts/vet-mz021-replica-hot-handoff.sh
GIT_INDEX_FILE="$isolated_index" git diff --cached --check
printf '%s\n' 'Staged MZ-021 paths:'
GIT_INDEX_FILE="$isolated_index" git diff --cached --name-only
GIT_INDEX_FILE="$isolated_index" git commit -m 'feat: add MZ-021 replica hot handoff'
