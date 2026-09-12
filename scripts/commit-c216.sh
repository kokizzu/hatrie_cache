#!/usr/bin/env bash
set -euo pipefail

expected_base="${C216_EXPECTED_BASE:-885f580d26e06df8e7e9e0246718423a92ebacee}"
actual_base="$(git rev-parse HEAD)"
if [[ "$actual_base" != "$expected_base" ]]; then
	printf 'refusing C216 commit: expected base %s, got %s\n' "$expected_base" "$actual_base" >&2
	exit 1
fi

if [[ -n "$(git diff --cached --name-only)" ]]; then
	printf '%s\n' 'refusing C216 commit: index is already staged'
	exit 1
fi

allowed_paths=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	C216_COLUMNAR_DICTIONARY_SHAPES.md
	INSPIRATION_ROUND2.md
	Makefile
	README.md
	hat/hatSql/contracts.go
	hat/hatSql/c216_columnar_dictionary_shape_test.go
	scripts/benchmark-c216.sh
	scripts/commit-c216.sh
	scripts/format-c216.sh
	scripts/push-c216.sh
	scripts/test-c216-full.sh
	scripts/test-c216-package.sh
	scripts/test-c216-race.sh
	scripts/test-c216.sh
	scripts/vet-c216.sh
)

is_allowed_path() {
	local candidate=$1
	local allowed
	for allowed in "${allowed_paths[@]}"; do
		if [[ "$candidate" == "$allowed" ]]; then
			return 0
		fi
	done
	return 1
}

status="$(git status --porcelain --untracked-files=all)"
while IFS= read -r line; do
	[[ -z "$line" ]] && continue
	path="${line:3}"
	if [[ "$path" == *' -> '* ]]; then
		path="${path##* -> }"
	fi
	if ! is_allowed_path "$path"; then
		printf 'refusing C216 commit: unexpected worktree path %s\n' "$path" >&2
		exit 1
	fi
done <<< "$status"

git diff --check
git add -- "${allowed_paths[@]}"
git diff --cached --check
git commit -m 'feat(hatSql): select columnar dictionaries by lookup shape'
