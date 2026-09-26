#!/usr/bin/env bash
set -euo pipefail

if ! git diff --cached --quiet; then
	printf '%s\n' 'refusing to stage CH-048 BETWEEN while unrelated changes are already staged' >&2
	exit 1
fi

base=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-ch048-between-makefile-base.XXXXXX")
candidate=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-ch048-between-makefile-candidate.XXXXXX")
patch=$(mktemp "${TMPDIR:-/tmp}/hatrie-cache-ch048-between-makefile-patch.XXXXXX")
cleanup() {
	rm -f "$base" "$candidate" "$patch"
}
trap cleanup EXIT

git show HEAD:Makefile > "$base"
cp "$base" "$candidate"
printf '%s\n' \
	'' \
	'.PHONY: benchmark-ch048-between' \
	'benchmark-ch048-between:' \
	$'\tbash scripts/benchmark-ch048-between.sh' \
	'' \
	'.PHONY: test-ch048-between' \
	'test-ch048-between:' \
	$'\tbash scripts/test-ch048-between.sh' \
	'' \
	'.PHONY: format-ch048-between' \
	'format-ch048-between:' \
	$'\tbash scripts/format-ch048-between.sh' \
	'' \
	'.PHONY: test-ch048-between-package race-ch048-between vet-ch048-between verify-ch048-between-docs review-ch048-between stage-ch048-between commit-ch048-between push-ch048-between' \
	'test-ch048-between-package:' \
	$'\tbash scripts/test-ch048-between-package.sh' \
	'race-ch048-between:' \
	$'\tbash scripts/race-ch048-between.sh' \
	'vet-ch048-between:' \
	$'\tbash scripts/vet-ch048-between.sh' \
	'verify-ch048-between-docs:' \
	$'\tbash scripts/verify-ch048-between-docs.sh' \
	'review-ch048-between:' \
	$'\tbash scripts/review-ch048-between.sh' \
	'stage-ch048-between:' \
	$'\tbash scripts/stage-ch048-between.sh' \
	'commit-ch048-between:' \
	$'\tbash scripts/commit-ch048-between.sh' \
	'push-ch048-between:' \
	$'\tbash scripts/push-ch048-between.sh' >> "$candidate"

if diff -u "$base" "$candidate" > "$patch"; then
	printf '%s\n' 'generated CH-048 BETWEEN Makefile patch is empty' >&2
	exit 1
else
	diff_status=$?
	if [ "$diff_status" -ne 1 ]; then
		exit "$diff_status"
	fi
fi
sed -i '1c\\--- a/Makefile' "$patch"
sed -i '2c\\+++ b/Makefile' "$patch"
git apply --cached "$patch"

git add \
	ENGINE_IDEAS.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	BENCHMARK.md \
	CH048_BETWEEN_PREDICATE.md \
	hat/hatSql/columnar_dictionary_predicate.go \
	hat/hatSql/query.go \
	hat/hatSql/ch048_between_predicate_test.go \
	scripts/benchmark-ch048-between.sh \
	scripts/test-ch048-between.sh \
	scripts/test-ch048-between-package.sh \
	scripts/format-ch048-between.sh \
	scripts/race-ch048-between.sh \
	scripts/vet-ch048-between.sh \
	scripts/verify-ch048-between-docs.sh \
	scripts/review-ch048-between.sh \
	scripts/stage-ch048-between.sh \
	scripts/commit-ch048-between.sh \
	scripts/push-ch048-between.sh
