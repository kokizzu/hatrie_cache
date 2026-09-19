#!/usr/bin/env bash
set -euo pipefail

index_file=$(mktemp /tmp/hatrie-cache-chu36-index.XXXXXX)
temp_dir=$(mktemp -d /tmp/hatrie-cache-chu36-commit.XXXXXX)

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

.PHONY: test-chu36
test-chu36:
	bash ./scripts/test-chu36.sh

.PHONY: benchmark-chu36
benchmark-chu36:
	bash ./scripts/benchmark-chu36.sh

.PHONY: format-chu36
format-chu36:
	bash ./scripts/format-chu36.sh

.PHONY: test-chu36-package
test-chu36-package:
	bash ./scripts/test-chu36-package.sh

.PHONY: race-chu36
race-chu36:
	bash ./scripts/race-chu36.sh

.PHONY: vet-chu36
vet-chu36:
	bash ./scripts/vet-chu36.sh

.PHONY: verify-chu36
verify-chu36:
	bash ./scripts/verify-chu36.sh

.PHONY: commit-chu36
commit-chu36:
	bash ./scripts/commit-chu36.sh

.PHONY: push-chu36
push-chu36:
	bash ./scripts/push-chu36.sh
EOF

git show HEAD:README.md > "$temp_dir/README.md"
insert_after \
	'[CHU35_OPTIMIZE_CONTROL.md](CHU35_OPTIMIZE_CONTROL.md).' \
	'- Opt-in bounded ClickHouse-style `system.parts` metadata with deterministic ordering, checksums, key ranges, and retention fields: [CHU36_SYSTEM_PARTS_CATALOG.md](CHU36_SYSTEM_PARTS_CATALOG.md), with measurements in [BENCHMARK.md](BENCHMARK.md#ch-u36-stable-sql-system-parts-catalog)' \
	"$temp_dir/README.md"

git show HEAD:PRODUCT_IDEA_GAPS.md > "$temp_dir/PRODUCT_IDEA_GAPS.md"
product_row='| CH-U36 | Complete `system.parts` catalog | Adopted as opt-in `SQLSystemPartProvider`: bounded, deterministic SQL rows expose part layout, checksum, key range, lifecycle, and retention metadata without paths or values; the legacy trie count view remains unchanged. | Bounded output, secret-free fields, and schema stability are covered by focused validation and provider-error tests. See [CHU36_SYSTEM_PARTS_CATALOG.md](CHU36_SYSTEM_PARTS_CATALOG.md) and [BENCHMARK.md](BENCHMARK.md#ch-u36-stable-sql-system-parts-catalog). |'
awk -v replacement="$product_row" '
	index($0, "| CH-U36 |") == 1 { print replacement; found = 1; next }
	{ print }
	END { if (!found) exit 1 }
' "$temp_dir/PRODUCT_IDEA_GAPS.md" > "$temp_dir/PRODUCT_IDEA_GAPS.md.next"
mv "$temp_dir/PRODUCT_IDEA_GAPS.md.next" "$temp_dir/PRODUCT_IDEA_GAPS.md"

git show HEAD:BENCHMARK.md > "$temp_dir/BENCHMARK.md"
cat >> "$temp_dir/BENCHMARK.md" <<'EOF'

## CH-U36 Stable SQL System Parts Catalog

The benchmark uses 64 local partitions and 4,096 seeded keys for the legacy
path, and 64 rich provider rows for the opt-in path. Five `-benchmem` samples
were run through `make benchmark-chu36` on Linux/amd64 with an AMD Ryzen 9
5950X.

| Path | Raw ns/op samples | Median ns/op | Median B/op | Median allocs/op | Relative CPU |
| --- | --- | ---: | ---: | ---: | ---: |
| Before: legacy view | 29,191; 28,117; 29,546; 30,096; 30,801 | 29,546 | 24,635 | 258 | 1.00x |
| After: legacy fast path | 25,932; 25,161; 25,498; 25,610; 25,810 | 25,610 | 24,635 | 258 | 0.87x |
| After: rich provider | 47,946; 48,053; 47,965; 46,713; 48,268 | 47,965 | 59,128 | 517 | 1.62x |

The default path is about 1.15x faster with unchanged allocations in this
run. The opt-in provider path costs about 1.87x the legacy CPU, 2.40x the
transient heap, and 2.00x the allocations because it copies, sorts, validates,
and materializes 64 metadata rows. It adds no cost to ordinary writes or to
resolvers without `PartProvider`.

Details and the complete column contract are in
[CHU36_SYSTEM_PARTS_CATALOG.md](CHU36_SYSTEM_PARTS_CATALOG.md).
EOF

git show HEAD:api.go > "$temp_dir/api.go"
insert_after \
	'type SQLSystemTablesResolverOptions = core.SQLSystemTablesResolverOptions' \
	'type SQLSystemPart = core.SQLSystemPart' \
	"$temp_dir/api.go"
insert_after \
	'type SQLSystemPart = core.SQLSystemPart' \
	'type SQLSystemPartProvider = core.SQLSystemPartProvider' \
	"$temp_dir/api.go"
insert_after \
	'type SQLSystemPartProvider = core.SQLSystemPartProvider' \
	'type SQLSystemPartProviderFunc = core.SQLSystemPartProviderFunc' \
	"$temp_dir/api.go"
insert_after \
	'const DefaultSQLSystemMutationLimit = core.DefaultSQLSystemMutationLimit' \
	'const DefaultSQLSystemPartLimit = core.DefaultSQLSystemPartLimit' \
	"$temp_dir/api.go"
insert_after \
	'const MaxSQLSystemMutationLimit = core.MaxSQLSystemMutationLimit' \
	'const MaxSQLSystemPartLimit = core.MaxSQLSystemPartLimit' \
	"$temp_dir/api.go"
insert_after \
	'const MaxSQLSystemPartLimit = core.MaxSQLSystemPartLimit' \
	'const MaxSQLSystemPartFieldBytes = core.MaxSQLSystemPartFieldBytes' \
	"$temp_dir/api.go"
insert_after \
	'const SQLSystemQueryHistoryTable = core.SQLSystemQueryHistoryTable' \
	'var ErrSQLSystemPartInvalid = core.ErrSQLSystemPartInvalid\n+var ErrSQLSystemPartsLimitExceeded = core.ErrSQLSystemPartsLimitExceeded' \
	"$temp_dir/api.go"

stage_temp_file Makefile "$temp_dir/Makefile"
stage_temp_file README.md "$temp_dir/README.md"
stage_temp_file PRODUCT_IDEA_GAPS.md "$temp_dir/PRODUCT_IDEA_GAPS.md"
stage_temp_file BENCHMARK.md "$temp_dir/BENCHMARK.md"
stage_temp_file api.go "$temp_dir/api.go"
stage_file CHU36_SYSTEM_PARTS_CATALOG.md
stage_file hat/hatCache/system_tables.go
stage_file hat/hatCache/chu36_system_parts_test.go
stage_file hat/hatCache/chu36_system_parts_benchmark_test.go
stage_file scripts/test-chu36.sh
stage_file scripts/benchmark-chu36.sh
stage_file scripts/format-chu36.sh
stage_file scripts/test-chu36-package.sh
stage_file scripts/race-chu36.sh
stage_file scripts/vet-chu36.sh
stage_file scripts/verify-chu36.sh
stage_file scripts/commit-chu36.sh
stage_file scripts/push-chu36.sh

printf '%s\n' 'CH-U36 staged paths:'
git diff --cached --name-only
git commit -m 'feat(sql): add bounded system parts catalog'
