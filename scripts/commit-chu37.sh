#!/usr/bin/env bash
set -euo pipefail

index_file=$(mktemp /tmp/hatrie-cache-chu37-index.XXXXXX)
temp_dir=$(mktemp -d /tmp/hatrie-cache-chu37-commit.XXXXXX)

cleanup() {
	unset GIT_INDEX_FILE
	rm -f "$index_file"
	rm -rf "$temp_dir"
}
trap cleanup EXIT

export GIT_INDEX_FILE="$index_file"
git read-tree HEAD

stage_file() {
	local path=$1
	local object
	object=$(git hash-object -w "$path")
	git update-index --add --cacheinfo "100644,$object,$path"
}

stage_temp_file() {
	local path=$1
	local source=$2
	local object
	object=$(git hash-object -w "$source")
	git update-index --add --cacheinfo "100644,$object,$path"
}

insert_after() {
	local pattern=$1
	local insertion=$2
	local file=$3
	local next="$file.next"
	awk -v pattern="$pattern" -v insertion="$insertion" '
		index($0, pattern) > 0 {
			print
			print insertion
			found = 1
			next
		}
		{ print }
		END { if (!found) exit 1 }
	' "$file" > "$next"
	mv "$next" "$file"
}

git show HEAD:Makefile > "$temp_dir/Makefile"
cat >> "$temp_dir/Makefile" <<'EOF'

.PHONY: test-chu37
test-chu37:
	bash ./scripts/test-chu37.sh

.PHONY: benchmark-chu37
benchmark-chu37:
	bash ./scripts/benchmark-chu37.sh

.PHONY: format-chu37
format-chu37:
	bash ./scripts/format-chu37.sh

.PHONY: test-chu37-package
test-chu37-package:
	bash ./scripts/test-chu37-package.sh

.PHONY: race-chu37
race-chu37:
	bash ./scripts/race-chu37.sh

.PHONY: vet-chu37
vet-chu37:
	bash ./scripts/vet-chu37.sh

.PHONY: verify-chu37
verify-chu37:
	bash ./scripts/verify-chu37.sh

.PHONY: commit-chu37
commit-chu37:
	bash ./scripts/commit-chu37.sh

.PHONY: push-chu37
push-chu37:
	bash ./scripts/push-chu37.sh
EOF

git show HEAD:README.md > "$temp_dir/README.md"
insert_after \
	'- Opt-in bounded ClickHouse-style `system.parts` metadata with deterministic ordering, checksums, key ranges, and retention fields: [CHU36_SYSTEM_PARTS_CATALOG.md](CHU36_SYSTEM_PARTS_CATALOG.md), with measurements in [BENCHMARK.md](BENCHMARK.md#ch-u36-stable-sql-system-parts-catalog)' \
	'- Opt-in bounded ClickHouse-style `system.mutations` metadata with stable IDs, affected parts, redacted errors, and lifecycle timestamps: [CHU37_SYSTEM_MUTATIONS_CATALOG.md](CHU37_SYSTEM_MUTATIONS_CATALOG.md), with measurements in [BENCHMARK.md](BENCHMARK.md#ch-u37-stable-sql-system-mutations-catalog)' \
	"$temp_dir/README.md"

git show HEAD:PRODUCT_IDEA_GAPS.md > "$temp_dir/PRODUCT_IDEA_GAPS.md"
product_row='| CH-U37 | Complete `system.mutations` catalog | Adopted as opt-in `SQLSystemMutationProvider`: bounded deterministic rows expose stable IDs, sequences, lifecycle, affected parts, redacted errors, and UTC timestamps; the legacy journal tail remains unchanged. | Restart retention, bounded cardinality, and redaction are covered by focused provider tests. See [CHU37_SYSTEM_MUTATIONS_CATALOG.md](CHU37_SYSTEM_MUTATIONS_CATALOG.md) and [BENCHMARK.md](BENCHMARK.md#ch-u37-stable-sql-system-mutations-catalog). |'
awk -v replacement="$product_row" '
	index($0, "| CH-U37 |") == 1 { print replacement; found = 1; next }
	{ print }
	END { if (!found) exit 1 }
' "$temp_dir/PRODUCT_IDEA_GAPS.md" > "$temp_dir/PRODUCT_IDEA_GAPS.md.next"
mv "$temp_dir/PRODUCT_IDEA_GAPS.md.next" "$temp_dir/PRODUCT_IDEA_GAPS.md"

git show HEAD:BENCHMARK.md > "$temp_dir/BENCHMARK.md"
cat >> "$temp_dir/BENCHMARK.md" <<'EOF'

## CH-U37 Stable SQL System Mutations Catalog

The benchmark uses 256 rows. The legacy path reads a 256-entry journal tail;
the provider path materializes 256 rows with two affected parts each. Five
`-benchmem` samples were run through `make benchmark-chu37` on Linux/amd64
with an AMD Ryzen 9 5950X.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Before: legacy journal tail | 289,160; 333,756; 309,822; 319,661; 308,487 | 309,822 | 228,076 | 2,575 | 1.00x |
| After: legacy journal tail | 313,686; 296,551; 331,982; 304,737; 325,440 | 313,686 | 228,076 | 2,575 | 1.01x |
| After: rich provider | 278,617; 239,539; 243,983; 250,774; 235,639 | 243,983 | 252,426 | 2,565 | 0.79x |

The legacy path is allocation-neutral and within benchmark noise for CPU. The
rich provider path is about 1.29x faster than the legacy path and uses 1.11x
the transient heap for the additional metadata copy, validation, and sorting.
It uses 10 fewer allocations in this workload. The provider is opt-in, and no
default write or journal-retention path pays its cost.

Details and the redaction contract are in
[CHU37_SYSTEM_MUTATIONS_CATALOG.md](CHU37_SYSTEM_MUTATIONS_CATALOG.md).
EOF

git show HEAD:api.go > "$temp_dir/api.go"
insert_after \
	'type SQLSystemPartProviderFunc = core.SQLSystemPartProviderFunc' \
	'type SQLSystemMutation = core.SQLSystemMutation' \
	"$temp_dir/api.go"
insert_after \
	'type SQLSystemMutation = core.SQLSystemMutation' \
	'type SQLSystemMutationProvider = core.SQLSystemMutationProvider' \
	"$temp_dir/api.go"
insert_after \
	'type SQLSystemMutationProvider = core.SQLSystemMutationProvider' \
	'type SQLSystemMutationProviderFunc = core.SQLSystemMutationProviderFunc' \
	"$temp_dir/api.go"
insert_after \
	'const MaxSQLSystemPartFieldBytes = core.MaxSQLSystemPartFieldBytes' \
	'const MaxSQLSystemMutationParts = core.MaxSQLSystemMutationParts' \
	"$temp_dir/api.go"
insert_after \
	'var ErrSQLSystemPartsLimitExceeded = core.ErrSQLSystemPartsLimitExceeded' \
	'var ErrSQLSystemMutationInvalid = core.ErrSQLSystemMutationInvalid' \
	"$temp_dir/api.go"
insert_after \
	'var ErrSQLSystemMutationInvalid = core.ErrSQLSystemMutationInvalid' \
	'var ErrSQLSystemMutationsLimitExceeded = core.ErrSQLSystemMutationsLimitExceeded' \
	"$temp_dir/api.go"

stage_temp_file Makefile "$temp_dir/Makefile"
stage_temp_file README.md "$temp_dir/README.md"
stage_temp_file PRODUCT_IDEA_GAPS.md "$temp_dir/PRODUCT_IDEA_GAPS.md"
stage_temp_file BENCHMARK.md "$temp_dir/BENCHMARK.md"
stage_temp_file api.go "$temp_dir/api.go"
stage_file CHU37_SYSTEM_MUTATIONS_CATALOG.md
stage_file hat/hatCache/system_tables.go
stage_file hat/hatCache/chu37_system_mutations_test.go
stage_file hat/hatCache/chu37_system_mutations_benchmark_test.go
stage_file scripts/test-chu37.sh
stage_file scripts/benchmark-chu37.sh
stage_file scripts/format-chu37.sh
stage_file scripts/test-chu37-package.sh
stage_file scripts/race-chu37.sh
stage_file scripts/vet-chu37.sh
stage_file scripts/verify-chu37.sh
stage_file scripts/commit-chu37.sh
stage_file scripts/push-chu37.sh

printf '%s\n' 'CH-U37 staged paths:'
git diff --cached --name-only
git commit -m 'feat(sql): add bounded system mutations catalog'
