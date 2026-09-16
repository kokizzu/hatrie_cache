#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet --; then
	printf '%s\n' 'refusing to stage with pre-existing staged changes' >&2
	exit 1
fi

temporary=$(mktemp -d /tmp/hatrie-cache-ch031-stage.XXXXXX)
cleanup() {
	rm -rf "$temporary"
}
trap cleanup EXIT

printf '%s\n' '| ClickHouse | Automatic typed JSON subcolumn promotion | Adopted as an opt-in bounded source primitive | `JSONSubcolumnAutoMaterializer` promotes repeated scalar paths after three observations, keys them by caller-supplied source generation, and bounds entries, rows, and retained typed payload. Cold, mixed-type, oversized, and stale paths fall back without retaining raw documents. Cached `Observe` is 28,925x faster than rematerializing the 4,096-row fixture with 0 B/op and 0 allocs/op; cached batch resolution is 6,589x faster with 752 B/op and 4 allocs/op. See [CH031_AUTOMATIC_TYPED_JSON_SUBCOLUMNS.md](CH031_AUTOMATIC_TYPED_JSON_SUBCOLUMNS.md) and [BENCHMARK.md](BENCHMARK.md#ch-031-automatic-typed-json-subcolumn-promotion). |' > "$temporary/adopted-row"
git show HEAD:ADOPTED_QUERY_ENGINE_IDEAS.md > "$temporary/adopted-head"
awk -v block="$temporary/adopted-row" '
	!inserted && index($0, "| ClickHouse | Typed scalar JSON subcolumns |") == 1 {
		print
		while ((getline line < block) > 0) print line
		close(block)
		inserted = 1
		next
	}
	{ print }
' "$temporary/adopted-head" > "$temporary/adopted"
adopted_blob=$(git hash-object -w "$temporary/adopted")

git show HEAD:Makefile > "$temporary/makefile"
printf '%s\n' \
	'' \
	'.PHONY: format-ch031-automatic-json-subcolumns' \
	'format-ch031-automatic-json-subcolumns:' \
	$'\tbash ./scripts/format-ch031-automatic-json-subcolumns.sh' \
	'.PHONY: review-ch031-automatic-json-subcolumns' \
	'review-ch031-automatic-json-subcolumns:' \
	$'\tbash ./scripts/review-ch031-automatic-json-subcolumns.sh' \
	'.PHONY: stage-ch031-automatic-json-subcolumns' \
	'stage-ch031-automatic-json-subcolumns:' \
	$'\tbash ./scripts/stage-ch031-automatic-json-subcolumns.sh' \
	'.PHONY: test-ch031-automatic-json-subcolumns' \
	'test-ch031-automatic-json-subcolumns:' \
	$'\tbash ./scripts/run-ch031-automatic-json-subcolumns.sh test' \
	'.PHONY: verify-ch031-automatic-json-subcolumns' \
	'verify-ch031-automatic-json-subcolumns:' \
	$'\tbash ./scripts/run-ch031-automatic-json-subcolumns.sh verify' \
	'.PHONY: benchmark-ch031-automatic-json-subcolumns' \
	'benchmark-ch031-automatic-json-subcolumns:' \
	$'\tbash ./scripts/run-ch031-automatic-json-subcolumns.sh benchmark' >> "$temporary/makefile"
makefile_blob=$(git hash-object -w "$temporary/makefile")

git add -- \
	README.md \
	BENCHMARK.md \
	INSPIRATION_BACKLOG.md \
	CH031_AUTOMATIC_TYPED_JSON_SUBCOLUMNS.md \
	hat/hatSql/ch031_automatic_json_subcolumn.go \
	hat/hatSql/ch031_automatic_json_subcolumn_test.go \
	hat/hatSql/ch031_automatic_json_subcolumn_benchmark_test.go \
	scripts/format-ch031-automatic-json-subcolumns.sh \
	scripts/review-ch031-automatic-json-subcolumns.sh \
	scripts/run-ch031-automatic-json-subcolumns.sh \
	scripts/stage-ch031-automatic-json-subcolumns.sh
git update-index --add --cacheinfo "100644,$adopted_blob,ADOPTED_QUERY_ENGINE_IDEAS.md"
git update-index --add --cacheinfo "100644,$makefile_blob,Makefile"
git diff --cached --check
git diff --cached --stat
