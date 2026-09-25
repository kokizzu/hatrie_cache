#!/usr/bin/env bash
set -euo pipefail

if [[ -n "$(git diff --cached --name-only)" ]]; then
	printf '%s\n' 'refusing to stage M052ad: the index already contains changes' >&2
	exit 1
fi

stage_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-m052ad-stage.XXXXXX")
trap 'rm -rf "$stage_dir"' EXIT

git show HEAD:INSPIRATION.md > "$stage_dir/INSPIRATION.md"
awk '
{
	print
	if ($0 == "  [M052AA_NATIVE_HASH_JOIN.md](M052AA_NATIVE_HASH_JOIN.md).") {
		print "- [x] M052ad Automatic safe native conditional aggregates. Supported"
		print "  ClickHouse-style `COUNT_IF`/`COUNTIF`, `SUM_IF`/`SUMIF`, `AVG_IF`/`AVGIF`,"
		print "  `MIN_IF`/`MINIF`, and `MAX_IF`/`MAXIF` forms reuse the native grouped"
		print "  aggregate state; filtered aggregate expressions and unsupported shapes keep"
		print "  the correct fallback. See"
		print "  [M052AD_AUTO_NATIVE_CONDITIONAL_AGGREGATES.md](M052AD_AUTO_NATIVE_CONDITIONAL_AGGREGATES.md)"
		print "  and [BENCHMARK.md](BENCHMARK.md#m052ad-automatic-native-conditional-aggregates)."
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
	if ($0 == "[M052AA_NATIVE_HASH_JOIN.md](M052AA_NATIVE_HASH_JOIN.md).") {
		print ""
		print "<a id=\"m052ad-automatic-native-conditional-aggregates\"></a>"
		print "## M052ad: Automatic Native Conditional Aggregates"
		print ""
		print "`make benchmark-m052ad-conditional-aggregate` compares the correct materialized"
		print "fallback with automatic native grouped execution for ClickHouse-style"
		print "conditional aggregates. The fixture has 20,000 rows, 257 groups, one"
		print "conditional count, and one conditional sum. Linux/amd64, AMD Ryzen 9 5950X,"
		print "three samples per path:"
		print ""
		print "```text"
		print "fallback:"
		print "17817491 ns/op 31145881 B/op 137758 allocs/op"
		print "17801243 ns/op 31145692 B/op 137757 allocs/op"
		print "17711667 ns/op 31145461 B/op 137758 allocs/op"
		print "automatic:"
		print "5194889 ns/op 7622228 B/op 60881 allocs/op"
		print "5286292 ns/op 7622226 B/op 60881 allocs/op"
		print "5303246 ns/op 7622228 B/op 60881 allocs/op"
		print "```"
		print ""
		print "| Path | Median time | Median bytes | Median allocs | Relative result |"
		print "| --- | ---: | ---: | ---: | --- |"
		print "| Correct materialized fallback | 17.817 ms/op | 31,145,692 B/op | 137,758 | 1.00x |"
		print "| Automatic native conditional aggregates | 5.286 ms/op | 7,622,228 B/op | 60,881 | **3.37x faster; 4.09x lower bytes; 2.26x fewer allocations** |"
		print ""
		print "The grouped hash fast path now rejects filtered aggregate expressions instead of"
		print "silently ignoring their filters. The old misclassified path was incorrect and"
		print "is not a valid performance baseline. Native selection is restricted to proven"
		print "scalar conditional forms; `DisableNativeDataflow` remains the explicit"
		print "fallback switch. See"
		print "[M052AD_AUTO_NATIVE_CONDITIONAL_AGGREGATES.md](M052AD_AUTO_NATIVE_CONDITIONAL_AGGREGATES.md)."
		inserted = 1
	}
}
END {
	if (!inserted) {
		exit 1
	}
}' "$stage_dir/BENCHMARK.md" > "$stage_dir/BENCHMARK.next"

git show HEAD:Makefile > "$stage_dir/Makefile"
printf '\n' >> "$stage_dir/Makefile"
printf '%s\n' \
  '.PHONY: format-m052ad-conditional-aggregate' \
  'format-m052ad-conditional-aggregate:' \
  $'\tbash scripts/format-m052ad-conditional-aggregate.sh' \
  '' \
  '.PHONY: test-m052ad-conditional-aggregate' \
  'test-m052ad-conditional-aggregate:' \
  $'\tbash scripts/test-m052ad-conditional-aggregate.sh' \
  '' \
  '.PHONY: benchmark-m052ad-conditional-aggregate' \
  'benchmark-m052ad-conditional-aggregate:' \
  $'\tbash scripts/benchmark-m052ad-conditional-aggregate.sh' \
  '' \
  '.PHONY: test-m052ad-package' \
  'test-m052ad-package:' \
  $'\tbash scripts/test-m052ad-package.sh' \
  '' \
  '.PHONY: race-m052ad-package' \
  'race-m052ad-package:' \
  $'\tbash scripts/race-m052ad-package.sh' \
  '' \
  '.PHONY: vet-m052ad-package' \
  'vet-m052ad-package:' \
  $'\tbash scripts/vet-m052ad-package.sh' \
  '' \
  '.PHONY: stage-m052ad-conditional-aggregate' \
  'stage-m052ad-conditional-aggregate:' \
  $'\tbash scripts/stage-m052ad-conditional-aggregate.sh' \
  '' \
  '.PHONY: commit-m052ad-conditional-aggregate' \
  'commit-m052ad-conditional-aggregate:' \
  $'\tbash scripts/commit-m052ad-conditional-aggregate.sh' \
  '' \
  '.PHONY: push-m052ad-conditional-aggregate' \
  'push-m052ad-conditional-aggregate:' \
  $'\tbash scripts/push-m052ad-conditional-aggregate.sh' \
  >> "$stage_dir/Makefile"

inspiration_blob=$(git hash-object -w "$stage_dir/INSPIRATION.next")
benchmark_blob=$(git hash-object -w "$stage_dir/BENCHMARK.next")
makefile_blob=$(git hash-object -w "$stage_dir/Makefile")
git update-index --add --cacheinfo 100644 "$inspiration_blob" INSPIRATION.md
git update-index --add --cacheinfo 100644 "$benchmark_blob" BENCHMARK.md
git update-index --add --cacheinfo 100644 "$makefile_blob" Makefile

git add -- \
	M052AD_AUTO_NATIVE_CONDITIONAL_AGGREGATES.md \
	hat/hatSql/m052c_native_dataflow.go \
	hat/hatSql/query.go \
	hat/hatSql/m052ad_auto_native_conditional_aggregate_test.go \
	scripts/benchmark-m052ad-conditional-aggregate.sh \
	scripts/commit-m052ad-conditional-aggregate.sh \
	scripts/format-m052ad-conditional-aggregate.sh \
	scripts/push-m052ad-conditional-aggregate.sh \
	scripts/race-m052ad-package.sh \
	scripts/stage-m052ad-conditional-aggregate.sh \
	scripts/test-m052ad-conditional-aggregate.sh \
	scripts/test-m052ad-package.sh \
	scripts/vet-m052ad-package.sh

git diff --cached --check
git diff --cached --name-only
