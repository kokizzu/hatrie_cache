#!/bin/sh
set -eu

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to stage M090c: the index already contains changes' >&2
	exit 1
fi

stage_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m090c-stage.XXXXXX")
trap 'rm -rf "$stage_dir"' EXIT

git show HEAD:INSPIRATION.md > "$stage_dir/INSPIRATION.md"
awk '
{
	print
	if ($0 == "  [BENCHMARK.md#m090b-context-aware-materialized-source-resolver](BENCHMARK.md#m090b-context-aware-materialized-source-resolver).") {
		print "- [x] M090c Projected materialized source resolution. \`hatSql.ProjectedSourceResolver\`"
		print "  lets compute nodes request only fields needed by conservative single-source"
		print "  queries, including the automatic native scalar path, while unsupported"
		print "  shapes and legacy resolvers fall back unchanged. See"
		print "  [M090C_PROJECTED_SOURCE.md](M090C_PROJECTED_SOURCE.md) and"
		print "  [BENCHMARK.md#m090c-projected-materialized-sources](BENCHMARK.md#m090c-projected-materialized-sources)."
		inserted = 1
	}
}
END {
	if (!inserted) {
		exit 1
	}
}' "$stage_dir/INSPIRATION.md" > "$stage_dir/INSPIRATION.next"

git show HEAD:BENCHMARK.md > "$stage_dir/BENCHMARK.md"
awk '
{
	print
	if ($0 == "\`make benchmark-m090b\`.") {
		print ""
		print "<a id=\"m090c-projected-materialized-sources\"></a>"
		print "## M090c Projected Materialized Sources"
		print ""
		print "This benchmark compares a full-row materialized resolver with the optional"
		print "\`hatSql.ProjectedSourceResolver\` on the same 4,096-row query. The query selects"
		print "\`id\` and filters on \`region\`; the source fixture also contains \`payload\`,"
		print "\`metadata\`, and \`unused\` fields. The projection path receives only \`id\` and"
		print "\`region\`. Three \`-benchmem\` samples were collected with"
		print "\`make benchmark-m090c-projected-source\` on \`linux/amd64\` with an AMD Ryzen 9"
		print "5950X. \`source-bytes/op\` is the source payload accounting added by the"
		print "benchmark; it is separate from local executor \`B/op\`."
		print ""
		print "### Raw Samples"
		print ""
		print "| Path | ns/op samples | source-bytes/op | B/op samples | allocs/op samples |"
		print "| --- | --- | ---: | --- | --- |"
		print "| Regular full-row resolver | 3,265,122; 3,280,698; 3,163,408 | 535,466 | 4,104,110; 4,104,078; 4,104,078 | 20,512; 20,512; 20,512 |"
		print "| Regular projected resolver | 2,576,344; 2,472,728; 3,278,391 | 56,234 | 4,104,255; 4,104,251; 4,104,266 | 20,516; 20,516; 20,516 |"
		print "| Native full-row resolver | 2,951,618; 2,167,342; 2,195,716 | 535,466 | 2,135,126; 2,135,111; 2,135,112 | 12,309; 12,309; 12,309 |"
		print "| Native projected resolver | 1,800,429; 1,843,344; 1,925,860 | 56,234 | 2,135,190; 2,135,226; 2,135,196 | 12,310; 12,310; 12,310 |"
		print ""
		print "### Median Comparison"
		print ""
		print "| Path | Median ns/op | Median source-bytes/op | Median B/op | Median allocs/op | Relative result |"
		print "| --- | ---: | ---: | ---: | ---: | --- |"
		print "| Regular full-row resolver | 3,265,122 | 535,466 | 4,104,078 | 20,512 | \`1.00x\` |"
		print "| Regular projected resolver | 2,576,344 | 56,234 | 4,104,255 | 20,516 | \`1.27x\` faster; \`9.52x\` lower source payload |"
		print "| Native full-row resolver | 2,195,716 | 535,466 | 2,135,112 | 12,309 | \`1.00x\` |"
		print "| Native projected resolver | 1,843,344 | 56,234 | 2,135,196 | 12,310 | \`1.19x\` faster; \`9.52x\` lower source payload |"
		print ""
		print "The executor heap is effectively unchanged, with four additional regular-path"
		print "allocations and one additional native-path allocation in this fixture. The"
		print "win is primarily reduced remote materialization and transfer work, not a"
		print "local row-map memory reduction. Unsupported query shapes and legacy resolvers"
		print "use the existing full-row path. Reproduce with \`make benchmark-m090c-projected-source\`."
		inserted = 1
	}
}
END {
	if (!inserted) {
		exit 1
	}
}' "$stage_dir/BENCHMARK.md" > "$stage_dir/BENCHMARK.next"

git show HEAD:SQL_COMPUTE_STORAGE_SEPARATION.md > "$stage_dir/SQL_COMPUTE_STORAGE_SEPARATION.md"
awk '
{
	print
	if ($0 == "[BENCHMARK.md#m090b-context-aware-materialized-source-resolver](BENCHMARK.md#m090b-context-aware-materialized-source-resolver).") {
		print ""
		print "## Projected Materialized Sources"
		print ""
		print "M090c adds the optional \`hatSql.ProjectedSourceResolver\` contract for a"
		print "materialized source that can fetch only selected fields. The SQL executor"
		print "automatically uses it for conservative single-source \`SELECT\` and \`WHERE\`"
		print "shapes, and the automatic native scalar dataflow path uses the same hook."
		print "\`ContextProjectedSourceResolver\` is preferred when the adapter also needs"
		print "request cancellation."
		print ""
		print "The field list contains parsed field identifiers only. Joins, aggregates,"
		print "ordering, windows, CTEs, unions, partition-aware resolvers, cached source"
		print "materializations, and unsupported expressions retain the ordinary full-row"
		print "path. Returning \`available=false\` is the compatibility escape hatch for"
		print "adapters that cannot project a particular source. Existing resolvers do not"
		print "need to change. See [M090C_PROJECTED_SOURCE.md](M090C_PROJECTED_SOURCE.md) for"
		print "the contract, example, and measured transport tradeoff."
		inserted = 1
	}
}
END {
	if (!inserted) {
		exit 1
	}
}' "$stage_dir/SQL_COMPUTE_STORAGE_SEPARATION.md" > "$stage_dir/SQL_COMPUTE_STORAGE_SEPARATION.next"

git show HEAD:Makefile > "$stage_dir/Makefile"
cat >> "$stage_dir/Makefile" <<'EOF'

.PHONY: format-m090c-projected-source
format-m090c-projected-source:
	sh scripts/format-m090c-projected-source.sh

.PHONY: test-m090c-projected-source
test-m090c-projected-source:
	sh scripts/test-m090c-projected-source.sh

.PHONY: benchmark-m090c-projected-source
benchmark-m090c-projected-source:
	sh scripts/benchmark-m090c-projected-source.sh

.PHONY: test-m090c-package
test-m090c-package:
	sh scripts/test-m090c-package.sh

.PHONY: race-m090c-package
race-m090c-package:
	sh scripts/race-m090c-package.sh

.PHONY: vet-m090c-package
vet-m090c-package:
	sh scripts/vet-m090c-package.sh

.PHONY: stage-m090c-projected-source
stage-m090c-projected-source:
	sh scripts/stage-m090c-projected-source.sh

.PHONY: commit-m090c-projected-source
commit-m090c-projected-source:
	sh scripts/commit-m090c-projected-source.sh

.PHONY: push-m090c-projected-source
push-m090c-projected-source:
	sh scripts/push-m090c-projected-source.sh
EOF

inspiration_blob=$(git hash-object -w "$stage_dir/INSPIRATION.next")
benchmark_blob=$(git hash-object -w "$stage_dir/BENCHMARK.next")
separation_blob=$(git hash-object -w "$stage_dir/SQL_COMPUTE_STORAGE_SEPARATION.next")
makefile_blob=$(git hash-object -w "$stage_dir/Makefile")
git update-index --add --cacheinfo 100644 "$inspiration_blob" INSPIRATION.md
git update-index --add --cacheinfo 100644 "$benchmark_blob" BENCHMARK.md
git update-index --add --cacheinfo 100644 "$separation_blob" SQL_COMPUTE_STORAGE_SEPARATION.md
git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile

git add -- \
	M090C_PROJECTED_SOURCE.md \
	hat/hatSql/catalog.go \
	hat/hatSql/contracts.go \
	hat/hatSql/m052p_auto_native_dataflow.go \
	hat/hatSql/m090c_projected_source.go \
	hat/hatSql/m090c_projected_source_test.go \
	hat/hatSql/query.go \
	hat/hatSql/session.go \
	scripts/benchmark-m090c-projected-source.sh \
	scripts/commit-m090c-projected-source.sh \
	scripts/format-m090c-projected-source.sh \
	scripts/push-m090c-projected-source.sh \
	scripts/race-m090c-package.sh \
	scripts/stage-m090c-projected-source.sh \
	scripts/test-m090c-package.sh \
	scripts/test-m090c-projected-source.sh \
	scripts/vet-m090c-package.sh

git diff --cached --check
git diff --cached --name-only
