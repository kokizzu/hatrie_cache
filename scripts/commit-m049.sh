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

stage=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m049-stage.XXXXXX")
archive=$(mktemp "${TMPDIR:-/tmp}/hatrie-m049-archive.XXXXXX.tar")
index=$(mktemp "${TMPDIR:-/tmp}/hatrie-m049-index.XXXXXX")
message=$(mktemp "${TMPDIR:-/tmp}/hatrie-m049-message.XXXXXX")
cleanup() {
	rm -rf "$stage" "$archive" "$index" "$message"
}
trap cleanup EXIT

git -C "$repo" archive --format=tar --output="$archive" "$base"
tar -xf "$archive" -C "$stage"

cat >>"$stage/Makefile" <<'EOF'

.PHONY: test
test:
	bash ./scripts/test-all.sh

.PHONY: format-m049 test-m049 benchmark-m049-baseline benchmark-m049
format-m049:
	bash ./scripts/format-m049.sh
test-m049:
	bash ./scripts/test-m049.sh
benchmark-m049-baseline:
	bash ./scripts/benchmark-m049-baseline.sh
benchmark-m049:
	bash ./scripts/benchmark-m049.sh

.PHONY: test-m049-package race-m049 vet-m049 verify-m049
test-m049-package:
	bash ./scripts/test-m049-package.sh
race-m049:
	bash ./scripts/race-m049.sh
vet-m049:
	bash ./scripts/vet-m049.sh
verify-m049:
	bash ./scripts/verify-m049.sh

.PHONY: commit-m049 push-m049
commit-m049:
	bash ./scripts/commit-m049.sh
push-m049:
	bash ./scripts/push-m049.sh
EOF

printf '\n- Dependency-aware SQL catalog migration planning and rollback orchestration: [M049_CATALOG_MIGRATION.md](M049_CATALOG_MIGRATION.md)\n' >>"$stage/README.md"
cat >>"$stage/BENCHMARK.md" <<'EOF'

## M-U49 Catalog Migration Runner

Command:

\`\`\`sh
make benchmark-m049
\`\`\`

Raw \`-count=5\` samples on Linux/amd64, AMD Ryzen 9 5950X:

| Workload | ns/op samples | Median ns/op | B/op | allocs/op |
| --- | --- | ---: | ---: | ---: |
| Three-step loop control | 12.48, 12.52, 12.27, 12.36, 12.33 | 12.36 | 0 | 0 |
| Validate, order, apply three steps | 1592, 1607, 1685, 1713, 1586 | 1607 | 848 | 12 |

The first row is a lower-bound control loop, not a previous catalog migration
implementation. The runner intentionally pays this bounded control-plane cost
to validate dependencies, produce deterministic ordering, track applied steps,
and retain rollback state. It should not be placed on a query or row-update
hot path.
EOF
perl -0pi -e 's{\| M-U49 \| Catalog migration runner \|[^\n]*\|[^\n]*\|}{| M-U49 | Catalog migration runner | Implemented in [M049_CATALOG_MIGRATION.md](M049_CATALOG_MIGRATION.md): dependency-aware validation/dry-run ordering, serial apply, and reverse rollback callbacks. | The caller still owns SQL locks, durable history, retries, engine-specific compatibility, and mixed-version rollout policy. |}' "$stage/PRODUCT_IDEA_GAPS.md"

feature_paths=(
	Makefile
	README.md
	BENCHMARK.md
	PRODUCT_IDEA_GAPS.md
	M049_CATALOG_MIGRATION.md
	hat/hatSql/m049_catalog_migration.go
	hat/hatSql/m049_catalog_migration_test.go
	hat/hatSql/m049_catalog_migration_public_test.go
	hat/hatSql/m049_catalog_migration_baseline_benchmark_test.go
	hat/hatSql/m049_catalog_migration_benchmark_test.go
	scripts/test-all.sh
	scripts/format-m049.sh
	scripts/test-m049.sh
	scripts/benchmark-m049-baseline.sh
	scripts/benchmark-m049.sh
	scripts/test-m049-package.sh
	scripts/race-m049.sh
	scripts/vet-m049.sh
	scripts/verify-m049.sh
	scripts/commit-m049.sh
	scripts/push-m049.sh
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
printf 'M-U49: add catalog migration runner\n\nDependency-aware dry-run ordering, serial apply, and reverse rollback callbacks.\n\nMeasured with make benchmark-m049; full verification runs through make test and make verify-m049.\n' >"$message"
commit=$(GIT_INDEX_FILE="$index" git -C "$repo" commit-tree "$tree" -p "$base" <"$message")
git -C "$repo" update-ref HEAD "$commit" "$current_head"
printf '%s\n' "$commit"
