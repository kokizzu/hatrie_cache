#!/usr/bin/env bash
set -euo pipefail

git add \
	CH051_LOW_CARDINALITY.md \
	BENCHMARK.md \
	README.md \
	INSPIRATION_BACKLOG.md \
	hat/hatDataStructure/low_cardinality.go \
	hat/hatDataStructure/low_cardinality_test.go \
	hat/hatDataStructure/low_cardinality_benchmark_test.go \
	scripts/test-ch051-c203.sh \
	scripts/format-ch051-c203.sh \
	scripts/benchmark-ch051-c203.sh \
	scripts/verify-ch051-c203.sh \
	scripts/stage-ch051-c203.sh \
	scripts/inspect-staged-ch051-c203.sh \
	scripts/commit-ch051-c203.sh \
	scripts/push-ch051-c203.sh

makefile_stage=$(mktemp)
trap 'rm -f "$makefile_stage"' EXIT
git show HEAD:Makefile > "$makefile_stage"
printf '%s\n' \
	'' \
	'test-ch051-c203:' \
	$'\tbash ./scripts/test-ch051-c203.sh' \
	'' \
	'format-ch051-c203:' \
	$'\tbash ./scripts/format-ch051-c203.sh' \
	'' \
	'benchmark-ch051-c203:' \
	$'\tBENCHMARK_PATTERN='\''$(BENCHMARK_PATTERN)'\'' BENCHMARK_COUNT='\''$(BENCHMARK_COUNT)'\'' bash ./scripts/benchmark-ch051-c203.sh' \
	'' \
	'benchmark-ch051-build-c203:' \
	$'\tBENCHMARK_PATTERN='\''Benchmark(LowCardinalityStringBuild|PlainStringColumnBuild)'\'' BENCHMARK_COUNT='\''3'\'' bash ./scripts/benchmark-ch051-c203.sh' \
	'' \
	'benchmark-ch051-build-stable-c203:' \
	$'\tBENCHMARK_PATTERN='\''Benchmark(LowCardinalityStringBuild|PlainStringColumnBuild)'\'' BENCHMARK_COUNT='\''5'\'' bash ./scripts/benchmark-ch051-c203.sh' \
	'' \
	'benchmark-ch051-group-c203:' \
	$'\tBENCHMARK_PATTERN='\''Benchmark(LowCardinalityStringGroupByCodes|LowCardinalityStringGroupByDenseCounts|PlainStringGroupByValues)'\'' BENCHMARK_COUNT='\''3'\'' bash ./scripts/benchmark-ch051-c203.sh' \
	'' \
	'benchmark-ch051-group-stable-c203:' \
	$'\tBENCHMARK_PATTERN='\''Benchmark(LowCardinalityStringGroupByCodes|LowCardinalityStringGroupByDenseCounts|PlainStringGroupByValues)'\'' BENCHMARK_COUNT='\''5'\'' bash ./scripts/benchmark-ch051-c203.sh' \
	'' \
	'benchmark-ch051-wire-c203:' \
	$'\tBENCHMARK_PATTERN='\''Benchmark(LowCardinalityStringMarshal|PlainStringMarshal)'\'' BENCHMARK_COUNT='\''3'\'' bash ./scripts/benchmark-ch051-c203.sh' \
	'' \
	'benchmark-ch051-wire-stable-c203:' \
	$'\tBENCHMARK_PATTERN='\''Benchmark(LowCardinalityStringMarshal|PlainStringMarshal)'\'' BENCHMARK_COUNT='\''5'\'' bash ./scripts/benchmark-ch051-c203.sh' \
	'' \
	'benchmark-ch051-high-cardinality-c203:' \
	$'\tBENCHMARK_PATTERN='\''Benchmark(LowCardinalityStringHighCardinalityBuild|PlainStringHighCardinalityBuild)'\'' BENCHMARK_COUNT='\''3'\'' bash ./scripts/benchmark-ch051-c203.sh' \
	'' \
	'benchmark-ch051-high-cardinality-stable-c203:' \
	$'\tBENCHMARK_PATTERN='\''Benchmark(LowCardinalityStringHighCardinalityBuild|PlainStringHighCardinalityBuild)'\'' BENCHMARK_COUNT='\''5'\'' bash ./scripts/benchmark-ch051-c203.sh' \
	'' \
	'verify-ch051-c203:' \
	$'\tbash ./scripts/verify-ch051-c203.sh' \
	'' \
	'stage-ch051-c203:' \
	$'\tbash ./scripts/stage-ch051-c203.sh' \
	'' \
	'inspect-staged-ch051-c203:' \
	$'\tbash ./scripts/inspect-staged-ch051-c203.sh' \
	'' \
	'commit-ch051-c203:' \
	$'\tbash ./scripts/commit-ch051-c203.sh' \
	'' \
	'push-ch051-c203:' \
	$'\tbash ./scripts/push-ch051-c203.sh' \
	>> "$makefile_stage"
makefile_blob=$(git hash-object -w "$makefile_stage")
git update-index --cacheinfo "100644,$makefile_blob,Makefile"
