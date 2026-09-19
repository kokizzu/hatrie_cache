#!/usr/bin/env bash
set -euo pipefail

repo=$(pwd)
git_dir="$repo/.git"
git -C "$repo" fetch origin master >/dev/null
base=$(git -C "$repo" rev-parse origin/master)
current_head=$(git -C "$repo" rev-parse HEAD)

if [[ "$current_head" != "$base" ]]; then
	printf 'refusing isolated commit: HEAD %s is not origin/master %s\n' "$current_head" "$base" >&2
	exit 1
fi

stage=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m050-stage.XXXXXX")
archive=$(mktemp "${TMPDIR:-/tmp}/hatrie-m050-archive.XXXXXX.tar")
index=$(mktemp "${TMPDIR:-/tmp}/hatrie-m050-index.XXXXXX")
message=$(mktemp "${TMPDIR:-/tmp}/hatrie-m050-message.XXXXXX")
cleanup() {
	rm -rf "$stage" "$archive" "$index" "$message"
}
trap cleanup EXIT

git -C "$repo" archive --format=tar --output="$archive" "$base"
tar -xf "$archive" -C "$stage"

if rg -q '^format-m050:' "$stage/Makefile"; then
	printf 'M-U50 targets already exist in base\n' >&2
	exit 1
fi

cat >>"$stage/Makefile" <<'EOF'

.PHONY: format-m050 test-m050 benchmark-m050-baseline benchmark-m050
format-m050:
	bash ./scripts/format-m050.sh
test-m050:
	bash ./scripts/test-m050.sh
benchmark-m050-baseline:
	bash ./scripts/benchmark-m050-baseline.sh
benchmark-m050:
	bash ./scripts/benchmark-m050.sh

.PHONY: test-m050-package race-m050 vet-m050 verify-m050
test-m050-package:
	bash ./scripts/test-m050-package.sh
race-m050:
	bash ./scripts/race-m050.sh
vet-m050:
	bash ./scripts/vet-m050.sh
verify-m050:
	bash ./scripts/verify-m050.sh

.PHONY: commit-m050 push-m050
commit-m050:
	bash ./scripts/commit-m050.sh
push-m050:
	bash ./scripts/push-m050.sh
EOF

printf '\n- Durable logical backup manifests joining storage, source offsets, frontiers, and subscriptions: [M050_DURABLE_FRONTIER_BACKUP.md](M050_DURABLE_FRONTIER_BACKUP.md)\n' >>"$stage/README.md"
cat >>"$stage/BENCHMARK.md" <<'EOF'

## M-U50 Durable Frontier-Based Backup

Commands:

~~~sh
make benchmark-m050-baseline
make benchmark-m050
~~~

Five runs on Linux/amd64, AMD Ryzen 9 5950X:

| Workload | ns/op samples | Median ns/op | B/op | allocs/op | Encoded size |
| --- | --- | ---: | ---: | ---: | ---: |
| Clean-tree control loop | 0.5477, 0.5508, 0.5261, 0.5600, 0.5360 | 0.5477 | 0 | 0 | n/a |
| Marshal plus restore-plan validation | 2058, 1930, 1935, 1982, 2008 | 1982 | 1488 | 27 | 117 bytes |

The control loop is a lower-bound measurement, not a previous backup
implementation. M-U50 is intentionally bounded control-plane work and should
not be put on a query or row-update hot path.
EOF
perl -0pi -e 's{\| M-U50 \| Durable frontier-based backup \|[^\n]*\|[^\n]*\|}{| M-U50 | Durable frontier-based backup | Implemented in [M050_DURABLE_FRONTIER_BACKUP.md](M050_DURABLE_FRONTIER_BACKUP.md): bounded deterministic binary manifest plus restore-order and journal-gap validation for storage, sources, frontiers, and subscriptions. | The caller still owns source barriers, file/checksum verification, journal replay, rehearsal, and durable publication. |}' "$stage/PRODUCT_IDEA_GAPS.md"

feature_paths=(
	Makefile
	README.md
	BENCHMARK.md
	PRODUCT_IDEA_GAPS.md
	M050_DURABLE_FRONTIER_BACKUP.md
	hat/hatBackup/m050_logical_snapshot.go
	hat/hatBackup/m050_logical_snapshot_test.go
	hat/hatBackup/m050_logical_snapshot_public_test.go
	hat/hatBackup/m050_logical_snapshot_baseline_benchmark_test.go
	hat/hatBackup/m050_logical_snapshot_benchmark_test.go
	scripts/format-m050.sh
	scripts/test-m050.sh
	scripts/benchmark-m050-baseline.sh
	scripts/benchmark-m050.sh
	scripts/test-m050-package.sh
	scripts/race-m050.sh
	scripts/vet-m050.sh
	scripts/verify-m050.sh
	scripts/commit-m050.sh
	scripts/push-m050.sh
)
for path in "${feature_paths[@]}"; do
	if [[ ! -f "$repo/$path" && ! -f "$stage/$path" ]]; then
		printf 'missing feature path: %s\n' "$path" >&2
		exit 1
	fi
done

for path in "${feature_paths[@]}"; do
	if [[ "$path" != "Makefile" && "$path" != "README.md" && "$path" != "BENCHMARK.md" && "$path" != "PRODUCT_IDEA_GAPS.md" ]]; then
		mkdir -p "$stage/$(dirname "$path")"
		cp "$repo/$path" "$stage/$path"
	fi
done

GIT_INDEX_FILE="$index" git -C "$repo" read-tree "$base"
GIT_INDEX_FILE="$index" git --git-dir="$git_dir" --work-tree="$stage" add -- "${feature_paths[@]}"
tree=$(GIT_INDEX_FILE="$index" git -C "$repo" write-tree)
printf 'M-U50: add durable frontier-based backup manifest\n\nJoin immutable storage identity, source offsets, frontiers, subscriptions, and a journal replay boundary with deterministic binary encoding and gap-safe restore planning.\n\nMeasured with make benchmark-m050; verified with make verify-m050 and make test.\n' >"$message"
commit=$(GIT_INDEX_FILE="$index" git -C "$repo" commit-tree "$tree" -p "$base" <"$message")
git -C "$repo" update-ref HEAD "$commit" "$current_head"
printf '%s\n' "$commit"

