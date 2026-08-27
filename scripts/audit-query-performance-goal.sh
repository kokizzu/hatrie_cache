#!/bin/sh
set -eu

if [ "${1:-}" = "details" ]; then
	printf '%s\n' 'SQL execution and index paths:'
	rg -n -C 3 -i 'bitmap|intersection|covering|column|late material|partition|skew|hot.key|rebuild' \
	  ./hat/hatCache/sql.go \
	  ./hat/hatCache/sql_query.go \
	  ./hat/hatSql/query.go || true

	printf '%s\n' '' 'Focused existing tests:'
	rg -n -C 2 '^func Test|intersection|covering|column|late material|partition|skew|hot.key|rebuild' \
	  ./hat/hatCache/sql_bitmap_index_test.go \
	  ./hat/hatCache/sql_partition_pruning_test.go \
	  ./hat/hatCache/sql_vector_test.go \
	  ./hat/hatStorage/reusable_indexes_test.go || true

	printf '%s\n' '' 'Spill and persistence paths:'
	rg -n -C 3 -i 'spill|bloom|compress|zstd|gzip|snappy' \
	  ./hat/hatSql \
	  ./hat/hatCache/leveldb_storage_format.go \
	  ./hat/hatCache/pebble_store.go \
	  ./hat/hatGrpc/compression.go || true
	exit 0
fi

if [ "${1:-}" = "indexes" ]; then
	printf '%s\n' 'Bitmap and JSON index implementation:'
	sed -n '320,760p' ./hat/hatCache/sql_query.go
	printf '%s\n' '' 'Bitmap index tests:'
	sed -n '1,320p' ./hat/hatCache/sql_bitmap_index_test.go
	printf '%s\n' '' 'SQL adapter index interfaces:'
	rg -n -C 4 'Index|index' ./hat/hatStorage/sql_adapter.go ./hat/hatStorage/reusable_indexes.go
	exit 0
fi

if [ "${1:-}" = "planner" ]; then
	printf '%s\n' 'Index resolver contracts and planning:'
	rg -n -C 12 'type SQL.*Index|ResolveSQL.*Index|resolveSQL.*Index|INDEX SCAN|COMPOSITE INDEX' ./hat/hatSql/*.go
	printf '%s\n' '' 'HatTrie SQL resolver interface methods:'
	rg -n -C 8 'ResolveSQL.*Index|ResolveSQL.*Source' ./hat/hatCache/sql_query.go
	exit 0
fi

if [ "${1:-}" = "bitmap" ]; then
	printf '%s\n' 'Roaring bitmap operations:'
	sed -n '1,300p' ./hat/hatDataStructure/roaring.go
	rg -n -C 3 'func \(.*RoaringBitmap.*\) (Intersect|Union|And|Or|Clone)' ./hat/hatDataStructure/roaring.go || true
	printf '%s\n' '' 'SQL resolver interfaces:'
	rg -n -C 8 'type .*Indexed.*Resolver|type .*SourceResolver' ./hat/hatSql/*.go
	exit 0
fi

printf '%s\n' 'Relevant implementation files:'
rg --files ./hat ./cmd ./scripts | rg '(sql|index|spill|partition|storage|benchmark|bench)' | sort

printf '%s\n' '' 'Existing capability references:'
rg -l -i 'intersection|covering index|bloom|compress|column.oriented|late material|partition prun|hot.key|skew|rebuild schedul|mixed.*workload' \
  ./hat ./cmd ./scripts ./*.md 2>/dev/null | sort || true

printf '%s\n' '' 'Related Make targets:'
rg -n '^(bench|benchmark|test.*sql|verify.*sql|audit.*sql|test.*partition|test.*storage|test.*spill|test.*index)' ./Makefile || true
