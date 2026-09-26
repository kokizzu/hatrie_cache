#!/usr/bin/env bash
set -euo pipefail

begin='# CH037_COLUMNAR_ARRAY_JOIN_FEATURE_TARGETS_BEGIN'
end='# CH037_COLUMNAR_ARRAY_JOIN_FEATURE_TARGETS_END'
tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/hatrie-ch037-stage.XXXXXX")"
trap 'rm -rf "$tmp_dir"' EXIT

if ! git diff --cached --quiet; then
	echo 'Refusing to stage CH037 with an already non-empty index.' >&2
	exit 1
fi

if git show HEAD:Makefile | grep -Fq "$begin"; then
	echo 'CH037 Makefile targets are already present in HEAD.' >&2
	exit 1
fi

awk -v begin="$begin" -v end="$end" '
	$0 == begin { found = 1 }
	found { print }
	$0 == end { exit }
' Makefile > "$tmp_dir/feature.mk"
if ! grep -Fq "$end" "$tmp_dir/feature.mk"; then
	echo 'CH037 Makefile target block is missing or incomplete.' >&2
	exit 1
fi

git show HEAD:Makefile > "$tmp_dir/base.mk"
awk '{ print }' "$tmp_dir/feature.mk" > "$tmp_dir/feature.append"
awk '{ print }' "$tmp_dir/base.mk" > "$tmp_dir/candidate.mk"
awk '{ print }' "$tmp_dir/feature.append" >> "$tmp_dir/candidate.mk"
set +e
diff -u --label a/Makefile --label b/Makefile "$tmp_dir/base.mk" "$tmp_dir/candidate.mk" > "$tmp_dir/makefile.patch"
diff_status=$?
set -e
if test "$diff_status" -ne 1; then
	echo 'Failed to construct the isolated Makefile patch.' >&2
	exit "$diff_status"
fi
git apply --cached "$tmp_dir/makefile.patch"

git add -- \
	README.md \
	BENCHMARK.md \
	ENGINE_IDEAS.md \
	CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
	CH037_COLUMNAR_ARRAY_JOIN.md \
	hat/hatSql/query.go \
	hat/hatSql/columnar_array_join.go \
	hat/hatSql/ch037_columnar_array_join_test.go \
	scripts/benchmark-ch037-columnar-array-join.sh \
	scripts/format-ch037-columnar-array-join.sh \
	scripts/test-ch037-columnar-array-join.sh \
	scripts/test-ch037-columnar-array-join-package.sh \
	scripts/race-ch037-columnar-array-join.sh \
	scripts/vet-ch037-columnar-array-join.sh \
	scripts/verify-ch037-columnar-array-join-docs.sh \
	scripts/review-ch037-columnar-array-join.sh \
	scripts/stage-ch037-columnar-array-join.sh \
	scripts/commit-ch037-columnar-array-join.sh \
	scripts/push-ch037-columnar-array-join.sh

git diff --cached --check
git diff --cached --stat
git status --short
