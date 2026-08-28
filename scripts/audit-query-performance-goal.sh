#!/bin/sh
set -eu

if [ "${1:-}" = "covering-inspect" ]; then
	printf '%s\n' 'Covering-index implementation anchors:'
	rg -n -C 6 '^func resolveSQLIndexedSource|^func \(ht \*HatTrie\) ResolveSQLIndexedSource|^type sqlJSONBitmapIndex|^func \(ht \*HatTrie\) CreateSQLJSONBitmapIndex' ./hat/hatSql/query.go ./hat/hatCache/sql_query.go
	printf '\n%s\n' 'Bitmap index lifecycle and lookup:'
	sed -n '300,430p' ./hat/hatCache/sql_query.go
	sed -n '577,720p' ./hat/hatCache/sql_query.go
	printf '\n%s\n' 'Resolver contracts:'
	sed -n '1,180p' ./hat/hatSql/contracts.go
	printf '\n%s\n' 'Index resolver call sites:'
	rg -n -C 10 'resolveSQLIndexedSource\(' ./hat/hatSql/query.go
	sed -n '6650,6725p' ./hat/hatSql/query.go
	printf '\n%s\n' 'HatTrie index fields:'
	rg -n -C 6 'sqlJSONBitmapIndexes|sqlJSONCompositeIndexes|sqlIndexMu' ./hat/hatCache/*.go
	printf '\n%s\n' 'Index refresh helpers:'
	rg -n -C 8 '^func refreshSQLJSON(Bitmap|Field|Composite)Index|^func parseSQL' ./hat/hatCache/sql_query.go
	exit 0
fi

if [ "${1:-}" = "maintenance-inspect" ]; then
	printf '%s\n' 'Index maintenance and health references:'
	rg -n -C 8 -i 'refresh.*index|index.*health|rebuild|maintenance|stale' ./hat/hatCache/sql_query.go ./hat/hatCache/*index*_test.go
	exit 0
fi

if [ "${1:-}" = "spill-inspect" ]; then
	printf '%s\n' 'Spill, join, Bloom, and compression references:'
	rg -n -C 8 'newSQLSpill(Encoder|Decoder)|type sqlSpill|HASH JOIN|hashJoin|func .*Join' ./hat/hatSql/query.go
	rg -n -C 4 -i 'bloom|compress' ./hat/hatSql ./hat/hatCache/*sql*_test.go || true
	printf '\n%s\n' 'Query spill options:'
	rg -n -C 10 'type SQLQueryOptions|SpillCipher|MaxSpillBytes' ./hat/hatSql/*.go
	exit 0
fi

if [ "${1:-}" = "spill-codec-inspect" ]; then
	printf '%s\n' 'Spill options and codec call sites:'
	rg -n -C 12 'type SQLQueryOptions|SpillCipher|newSQLSpillEncoder\(' ./hat/hatSql/*.go
	exit 0
fi

if [ "${1:-}" = "spill-run-inspect" ]; then
	rg -n -C 5 'run := sqlSpill(Run|GroupRun)|file.Close\(\)|closeSQLSpillHashPartitions' ./hat/hatSql/query.go
	exit 0
fi

if [ "${1:-}" = "bloom-inspect" ]; then
	rg -n -C 8 -i 'type .*Bloom|func .*Bloom|New.*Bloom' ./hat/hatDataStructure ./hat/hatSql ./hat/hatCache
	sed -n '3607,3778p' ./hat/hatSql/query.go
	exit 0
fi

if [ "${1:-}" = "columnar-inspect" ]; then
	printf '%s\n' '=== batch expression evaluation ==='
	rg -n -C 4 'evalSQLExprBatch|sql.*Batch|batch.*SQL' ./hat/hatSql/query.go ./hat/hatSql/*test.go
	printf '%s\n' '=== source resolution and row construction ==='
	rg -n -C 4 'ResolveSQLSource|StreamSQLSource|sqlJSONRows|SQLJSON' ./hat/hatSql/query.go ./hat/hatCache/sql_query.go
	printf '%s\n' '=== projection, ordering, and row identity ==='
	rg -n -C 4 'projection|Projection|ordinal|rowID|rowId|sourceKey' ./hat/hatSql/query.go ./hat/hatSql/*test.go
	exit 0
fi

if [ "${1:-}" = "columnar-seams" ]; then
	printf '%s\n' '=== primary source planning ==='
	sed -n '6680,6775p' ./hat/hatSql/query.go
	printf '%s\n' '=== index and source resolver contracts ==='
	sed -n '8280,8500p' ./hat/hatSql/query.go
	printf '%s\n' '=== scan row envelopes ==='
	sed -n '8880,8975p' ./hat/hatSql/query.go
	printf '%s\n' '=== cache JSON source implementation ==='
	rg -n -C 8 'ResolveSQLSource|StreamSQLSource|sqlJSONRows' ./hat/hatCache/sql_query.go
	exit 0
fi

if [ "${1:-}" = "columnar-contracts" ]; then
	printf '%s\n' '=== exported resolver contracts ==='
	rg -n -C 3 '^type SQL.*(SourceResolver|Resolver)' ./hat/hatSql/query.go
	printf '%s\n' '=== ordered and streamed execution gates ==='
	rg -n -C 4 'executeSQLStreamed|resolveSQLOrderedSource|OrderedStream' ./hat/hatSql/query.go
	printf '%s\n' '=== cache resolver methods ==='
	rg -n -C 2 '^func \(ht \*HatTrie\) (ResolveSQL|StreamSQL)' ./hat/hatCache/sql_query.go
	exit 0
fi

if [ "${1:-}" = "columnar-tests" ]; then
	printf '%s\n' '=== covering index test conventions ==='
	sed -n '1,240p' ./hat/hatCache/sql_covering_index_test.go
	printf '%s\n' '=== index maintenance test conventions ==='
	sed -n '1,180p' ./hat/hatCache/sql_index_maintenance_test.go
	exit 0
fi

if [ "${1:-}" = "columnar-implementation-seams" ]; then
	printf '%s\n' '=== resolver definitions ==='
	rg -n -C 6 '^type (SourceResolver|StreamSourceResolver|OrderedSourceResolver|CoveringIndexedSourceResolver) interface' ./hat/hatSql
	printf '%s\n' '=== materialized executor entry and final projection ==='
	rg -n -C 10 '^func executeSQLQueryWithMetricsOuter|^func sqlProject' ./hat/hatSql/query.go
	printf '%s\n' '=== cache SQL imports and JSON decoding ==='
	sed -n '1,95p' ./hat/hatCache/sql_query.go
	sed -n '1560,1630p' ./hat/hatCache/sql_query.go
	exit 0
fi

if [ "${1:-}" = "columnar-ast" ]; then
	rg -n -C 8 '^type (sqlQuery|sqlExpr|sqlSelect|sqlSource) struct' ./hat/hatSql/query.go
	exit 0
fi

if [ "${1:-}" = "columnar-build-failure" ]; then
	sed -n '6750,6805p' ./hat/hatSql/query.go
	exit 0
fi

if [ "${1:-}" = "columnar-benchmark-fixture" ]; then
	rg -n -C 4 '^func newTestTrie|^func New|^func \(.*HatTrie.*Close' ./hat/hatCache/*_test.go ./hat/hatCache/main.go
	exit 0
fi

if [ "${1:-}" = "persisted-compression" ]; then
	printf '%s\n' '=== persisted compression and codec files ==='
	rg -l -i 'compress|codec|marshal|snapshot' ./hat/hatStorage ./hat/hatSnapshot ./hat/hatJournal ./hat/hatCache --glob '*.go'
	printf '%s\n' '=== durable storage compression references ==='
	rg -n -C 3 -i 'compress|codec|gzip|snappy|zstd' ./hat/hatStorage ./hat/hatSnapshot ./hat/hatJournal --glob '*.go'
	exit 0
fi

if [ "${1:-}" = "persisted-compression-storage-seams" ]; then
	printf '%s\n' '=== snapshot format and compression contract ==='
	sed -n '1,260p' ./hat/hatSnapshot/snapshot.go
	printf '%s\n' '=== persistent backend format and value envelope ==='
	rg -n -C 6 -i 'format|encode|decode|marshal|unmarshal|compress|encryption' ./hat/hatCache/leveldb_storage_format.go ./hat/hatCache/leveldb_store.go ./hat/hatCache/pebble_store.go ./hat/hatCache/storage_backend.go ./hat/hatCache/persistent_encryption.go
	exit 0
fi

if [ "${1:-}" = "partition-pruning" ]; then
	printf '%s\n' '=== partition pruning API and tests ==='
	rg -n -C 5 'SQLPartitionPruningPlan|Partition.*Prun|partition.*prun' ./hat/hatCache --glob '*.go'
	exit 0
fi

if [ "${1:-}" = "hot-key-skew" ]; then
	printf '%s\n' '=== skew and hot-key stats ==='
	rg -n -C 5 -i 'skew|hot.key|frequency|dominant' ./hat/hatCache/sql_query.go ./hat/hatSql/query.go ./hat/hatCache/*test.go ./hat/hatSql/*test.go
	exit 0
fi

if [ "${1:-}" = "aggregate-skew" ]; then
	printf '%s\n' '=== grouped aggregate state and spill control ==='
	rg -n -C 5 'grouped|groupBy|SpillGroup|GroupAggregate|max.*[Gg]roup|group.*[Ll]imit' ./hat/hatSql/query.go ./hat/hatSql/contracts.go
	exit 0
fi

if [ "${1:-}" = "time-partitioning" ]; then
	printf '%s\n' '=== time-series and partition public API ==='
	rg -n -C 5 'TimeSeries|time.series|ConfigureLocalPartitions|LocalPartition' ./hat/hatCache ./hat/hatPartition --glob '*.go'
	exit 0
fi

if [ "${1:-}" = "mixed-workload-benchmarks" ]; then
	printf '%s\n' '=== benchmark commands and workload fixtures ==='
	sed -n '1,260p' ./cmd/hatrie-sqlbench/main.go
	sed -n '1,180p' ./cmd/hatrie-sqlbench/main_test.go
	rg -n -C 4 -i 'mixed|workload|read.write' ./hat/hatCache --glob '*benchmark_test.go'
	exit 0
fi

if [ "${1:-}" = "time-series-pruning-seams" ]; then
	printf '%s\n' '=== time-series execution flow ==='
	rg -n -C 12 'func QuerySQLTimeSeries|type TimeSeriesOptions|func .*TimeSeries' ./hat/hatSql ./hat/hatCache
	exit 0
fi

if [ "${1:-}" = "time-series-bucket-contract" ]; then
	rg -n -C 8 'type TimeSeriesBucket struct|type TimeSeriesPoint struct' ./hat/hatDataStructure
	exit 0
fi

if [ "${1:-}" = "aggregate-skew-seams" ]; then
	printf '%s\n' '=== query options and aggregation materialization ==='
	rg -n -C 5 'type QueryOptions struct|type SQLQueryOptions' ./hat/hatSql
	rg -n -C 8 'groups :=|groupRows|GROUP BY|groupBy' ./hat/hatSql/query.go
	exit 0
fi

if [ "${1:-}" = "aggregate-skew-implementation" ]; then
	sed -n '165,240p' ./hat/hatSql/query.go
	sed -n '7325,7385p' ./hat/hatSql/query.go
	sed -n '10670,10750p' ./hat/hatSql/query.go
	exit 0
fi

if [ "${1:-}" = "time-partition-state" ]; then
	rg -n -C 10 'sqlIndexMu|sqlJSONIndexes|CreateHatTrie' ./hat/hatCache/main.go
	sed -n '3772,3855p' ./hat/hatCache/main.go
	exit 0
fi

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

if [ "${1:-}" = "covering" ]; then
	printf '%s\n' 'Covering-index references:'
	rg -n -C 8 -i 'covering|projection|project' ./hat/hatCache/sql_query.go ./hat/hatSql/query.go ./hat/hatCache/*index*_test.go || true
	printf '%s\n' '' 'Index source resolution and projection execution:'
	sed -n '8360,8470p' ./hat/hatSql/query.go
	sed -n '7100,7280p' ./hat/hatSql/query.go
	exit 0
fi

printf '%s\n' 'Relevant implementation files:'
rg --files ./hat ./cmd ./scripts | rg '(sql|index|spill|partition|storage|benchmark|bench)' | sort

printf '%s\n' '' 'Existing capability references:'
rg -l -i 'intersection|covering index|bloom|compress|column.oriented|late material|partition prun|hot.key|skew|rebuild schedul|mixed.*workload' \
  ./hat ./cmd ./scripts ./*.md 2>/dev/null | sort || true

printf '%s\n' '' 'Related Make targets:'
rg -n '^(bench|benchmark|test.*sql|verify.*sql|audit.*sql|test.*partition|test.*storage|test.*spill|test.*index)' ./Makefile || true
