#!/usr/bin/env bash
set -euo pipefail

git add -- \
    BENCHMARK.md \
    CLICKHOUSE_MATERIALIZE_TARANTOOL_AUDIT.md \
    INSPIRATION.md \
    M052_NATIVE_UNION.md \
    hat/hatSql/m052p_auto_native_dataflow.go \
    hat/hatSql/m052p_auto_native_dataflow_test.go \
    hat/hatSql/m052z_auto_native_union_benchmark_test.go \
    hat/hatSql/m052z_native_union.go \
    hat/hatSql/query.go \
    scripts/benchmark-m052-union-native.sh \
    scripts/commit-m052-union-native.sh \
    scripts/format-m052-union-native.sh \
    scripts/push-m052-union-native.sh \
    scripts/stage-m052-union-native.sh \
    scripts/test-m052-union-native.sh \
    scripts/verify-go-temp-cache.sh

head_make=$(mktemp)
feature_make=$(mktemp)
make_patch=$(mktemp)
cleanup() {
    rm -f "$head_make" "$feature_make" "$make_patch"
}
trap cleanup EXIT

git show HEAD:Makefile > "$head_make"
cp "$head_make" "$feature_make"
cat >> "$feature_make" <<'EOF'

.PHONY: test-m052-union-native
test-m052-union-native:
	bash ./scripts/test-m052-union-native.sh

.PHONY: benchmark-m052-union-native
benchmark-m052-union-native:
	bash ./scripts/benchmark-m052-union-native.sh

.PHONY: verify-go-temp-cache
verify-go-temp-cache:
	bash ./scripts/verify-go-temp-cache.sh

.PHONY: format-m052-union-native
format-m052-union-native:
	bash ./scripts/format-m052-union-native.sh

.PHONY: stage-m052-union-native
stage-m052-union-native:
	bash ./scripts/stage-m052-union-native.sh

.PHONY: commit-m052-union-native
commit-m052-union-native:
	bash ./scripts/commit-m052-union-native.sh

.PHONY: push-m052-union-native
push-m052-union-native:
	bash ./scripts/push-m052-union-native.sh
EOF

set +e
git diff --no-index -- "$head_make" "$feature_make" > "$make_patch"
diff_status=$?
set -e
if [[ "$diff_status" -ne 1 ]]; then
    printf 'unexpected Makefile patch status: %s\n' "$diff_status" >&2
    exit 1
fi

sed -i \
    -e 's|^diff --git .*|diff --git a/Makefile b/Makefile|' \
    -e 's|^--- .*|--- a/Makefile|' \
    -e 's|^+++ .*|+++ b/Makefile|' \
    "$make_patch"
git apply --cached --unidiff-zero "$make_patch"

git diff --cached --check
git diff --cached --stat
