#!/usr/bin/env bash
set -euo pipefail

mode=${1:-}
package_sql=./hat/hatSql
package_schema=./hat/hatSchema
package_cache=./hat/hatCache
test_pattern='TestTT024CrossFieldTextIndex|TestSQLContainsPhraseCrossField|TestSQLTextPhraseIndexCrossField'
benchmark_pattern='BenchmarkTT024CrossField'
cache=$(mktemp -d /tmp/hatrie-cache-tt024-cross-field-gocache.XXXXXX)
park=
baseline_paths=(
	hat/hatSql/contracts.go
	hat/hatSql/text_proximity.go
	hat/hatCache/sql_text_phrase.go
	hat/hatCache/monitoring.go
	hat/hatSchema/text_index.go
	hat/hatSchema/text_index_resolver.go
)
test_paths=(
	hat/hatSql/tt024_cross_field_text_test.go
	hat/hatSchema/tt024_cross_field_text_test.go
)
export GOCACHE="$cache"

cleanup() {
	for path in "${baseline_paths[@]}" "${test_paths[@]}"; do
		if [[ -n "$park" && -f "$park/$path" ]]; then
			rm -f "$path"
			mv "$park/$path" "$path"
		fi
	done
	if [[ -n "$park" ]]; then
		rm -rf "$park"
	fi
	rm -rf "$cache"
}
trap cleanup EXIT

case "$mode" in
baseline)
	park=$(mktemp -d /tmp/hatrie-cache-tt024-cross-field-baseline.XXXXXX)
	for path in "${baseline_paths[@]}" "${test_paths[@]}"; do
		mkdir -p "$park/$(dirname "$path")"
		mv "$path" "$park/$path"
	done
	for path in "${baseline_paths[@]}"; do
		git show "HEAD:$path" > "$path"
	done
	go test "$package_schema" -run '^$' -bench "$benchmark_pattern" -benchmem -count=5 | tee TT024_CROSS_FIELD_BENCHMARK_BASELINE_RAW.txt
	;;
format)
	gofmt -w \
		hat/hatSql/contracts.go \
		hat/hatSql/text_proximity.go \
		hat/hatSql/tt024_cross_field_text_test.go \
		hat/hatCache/sql_text_phrase.go \
		hat/hatCache/monitoring.go \
		hat/hatCache/tt024_cross_field_text_test.go \
		hat/hatSchema/text_index.go \
		hat/hatSchema/text_index_resolver.go \
		hat/hatSchema/tt024_cross_field_text_benchmark_test.go \
		hat/hatSchema/tt024_cross_field_text_test.go
	;;
test)
	go test "$package_sql" "$package_schema" "$package_cache" -run "$test_pattern" -count=1
	;;
benchmark)
	go test "$package_schema" -run '^$' -bench "$benchmark_pattern" -benchmem -count=5 | tee TT024_CROSS_FIELD_BENCHMARK_RAW.txt
	;;
race)
	go test -race "$package_sql" "$package_schema" "$package_cache" -run "$test_pattern" -count=1
	;;
vet)
	go vet "$package_sql" "$package_schema" "$package_cache"
	;;
package-test)
	go test "$package_sql" "$package_schema" "$package_cache"
	;;
check)
	if [[ -n "$(gofmt -d \
		hat/hatSql/contracts.go \
		hat/hatSql/text_proximity.go \
		hat/hatSql/tt024_cross_field_text_test.go \
		hat/hatCache/sql_text_phrase.go \
		hat/hatCache/monitoring.go \
		hat/hatCache/tt024_cross_field_text_test.go \
		hat/hatSchema/text_index.go \
		hat/hatSchema/text_index_resolver.go \
		hat/hatSchema/tt024_cross_field_text_benchmark_test.go \
		hat/hatSchema/tt024_cross_field_text_test.go)" ]]; then
		printf '%s\n' 'gofmt check failed' >&2
		exit 1
	fi
	go test "$package_sql" "$package_schema" "$package_cache" -run "$test_pattern" -count=1
	go test -race "$package_sql" "$package_schema" "$package_cache" -run "$test_pattern" -count=1
	go vet "$package_sql" "$package_schema" "$package_cache"
	git diff --check
	;;
status)
	git status --short
	;;
commit)
	git add \
		TT024_CROSS_FIELD_BENCHMARK_BASELINE_RAW.txt \
		TT024_CROSS_FIELD_BENCHMARK_RAW.txt \
		TT024_CROSS_FIELD_TEXT_INDEX.md \
		BENCHMARK.md \
		ADOPTED_QUERY_ENGINE_IDEAS.md \
		INSPIRATION.md \
		CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
		ENGINE_IDEAS.md \
		Makefile \
		hat/hatSql/contracts.go \
		hat/hatSql/text_proximity.go \
		hat/hatSql/tt024_cross_field_text_test.go \
		hat/hatCache/sql_text_phrase.go \
		hat/hatCache/monitoring.go \
		hat/hatCache/tt024_cross_field_text_test.go \
		hat/hatSchema/text_index.go \
		hat/hatSchema/text_index_resolver.go \
		hat/hatSchema/tt024_cross_field_text_benchmark_test.go \
		hat/hatSchema/tt024_cross_field_text_test.go \
		scripts/tt024-cross-field-text.sh
	git commit -m 'feat(sql): add cross-field text index unions [skip ci]'
	;;
push)
	git push -u origin HEAD
	;;
*)
	printf 'usage: %s baseline|format|test|benchmark|race|vet|package-test|check|status|commit|push\n' "$0" >&2
	exit 2
	;;
esac
