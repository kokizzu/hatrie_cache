#!/usr/bin/env bash
set -euo pipefail

feature_paths=(
	TT024_CROSS_FIELD_MIXED_BOOLEAN.md
	hat/hatSql/contracts.go
	hat/hatSql/text_proximity.go
	hat/hatSql/tt024_cross_field_test.go
	hat/hatSql/tt024_cross_field_benchmark_test.go
	scripts/benchmark-tt024-cross-field.sh
	scripts/inspect-tt024-cross-field.sh
	scripts/deliver-tt024-cross-field.sh
)

if ! git diff --cached --quiet; then
	echo 'Refusing delivery: the index already contains staged changes.' >&2
	exit 1
fi

makefile_patch=$(mktemp "${TMPDIR:-/tmp}/hatrie-tt024-cross-field-makefile.XXXXXX")
trap 'rm -f "$makefile_patch"' EXIT
printf '%s\n' \
	'--- Makefile' \
	'+++ Makefile' \
	'@@ -1,0 +1,10 @@' \
	'+.PHONY: inspect-tt024-cross-field benchmark-tt024-cross-field deliver-tt024-cross-field' \
	'+' \
	'+inspect-tt024-cross-field:' \
	'+\tbash ./scripts/inspect-tt024-cross-field.sh' \
	'+' \
	'+benchmark-tt024-cross-field:' \
	'+\tbash ./scripts/benchmark-tt024-cross-field.sh' \
	'+' \
	'+deliver-tt024-cross-field:' \
	'+\tbash ./scripts/deliver-tt024-cross-field.sh' \
	> "$makefile_patch"

git apply --cached --unidiff-zero "$makefile_patch"
git add -- "${feature_paths[@]}"

expected_paths=(
	Makefile
	"${feature_paths[@]}"
)
staged_paths=$(git diff --cached --name-only)
for path in "${expected_paths[@]}"; do
	if ! printf '%s\n' "$staged_paths" | grep -Fxq "$path"; then
		echo "Refusing delivery: expected staged path missing: $path" >&2
		exit 1
	fi
done
if [[ $(printf '%s\n' "$staged_paths" | wc -l) -ne ${#expected_paths[@]} ]]; then
	echo 'Refusing delivery: unexpected staged paths detected.' >&2
	printf '%s\n' "$staged_paths" >&2
	exit 1
fi

git diff --cached --check
git commit -m 'feat: add cross-field mixed text index unions [skip ci]'
git push origin HEAD
echo 'TT-024 cross-field delivery completed.'
