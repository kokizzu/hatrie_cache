#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
tmp_dir="$(mktemp -d)"
trap 'rm -rf "$tmp_dir"' EXIT

index_file="$tmp_dir/index"
makefile="$tmp_dir/Makefile"
inspiration="$tmp_dir/INSPIRATION.md"

feature_paths=(
	"TYPED_TABLE_MONOTONE.md"
	"hat/hatSql/typed_table_monotone.go"
	"hat/hatSql/typed_table_monotone_test.go"
	"scripts/test-sql-typed-table-monotone.sh"
	"scripts/test-race-sql-typed-table-monotone.sh"
	"scripts/format-sql-typed-table-monotone.sh"
	"scripts/benchmark-sql-typed-table-monotone.sh"
	"scripts/deliver-sql-typed-table-monotone.sh"
)

prepare_index() {
	git -C "$repo_root" show HEAD:Makefile > "$makefile"
	if ! grep -Fq 'test-sql-typed-table-monotone:' "$makefile"; then
		cat >> "$makefile" <<'EOF'

.PHONY: test-sql-typed-table-monotone
test-sql-typed-table-monotone:
	bash ./scripts/test-sql-typed-table-monotone.sh

.PHONY: test-race-sql-typed-table-monotone
test-race-sql-typed-table-monotone:
	bash ./scripts/test-race-sql-typed-table-monotone.sh

.PHONY: format-sql-typed-table-monotone
format-sql-typed-table-monotone:
	bash ./scripts/format-sql-typed-table-monotone.sh

.PHONY: benchmark-sql-typed-table-monotone
benchmark-sql-typed-table-monotone:
	bash ./scripts/benchmark-sql-typed-table-monotone.sh

.PHONY: deliver-sql-typed-table-monotone
deliver-sql-typed-table-monotone:
	bash ./scripts/deliver-sql-typed-table-monotone.sh preview

.PHONY: commit-sql-typed-table-monotone
commit-sql-typed-table-monotone:
	bash ./scripts/deliver-sql-typed-table-monotone.sh commit

.PHONY: push-sql-typed-table-monotone
push-sql-typed-table-monotone:
	bash ./scripts/deliver-sql-typed-table-monotone.sh push
EOF
	fi

	git -C "$repo_root" show HEAD:INSPIRATION.md > "$inspiration"
	awk '
		BEGIN {
			line = "- [x] M070a Append-only typed-table aggregate fast path."
			added = 0
		}
		$0 == line {
			added = 1
			print
			next
		}
		$0 ~ /M070[[:space:]].*[Mm]onotone aggregate/ && !added {
			print
			print line
			added = 1
			next
		}
		{ print }
		END {
			if (!added) {
				print ""
				print line
			}
		}
	' "$inspiration" > "$tmp_dir/INSPIRATION.generated.md"
	mv "$tmp_dir/INSPIRATION.generated.md" "$inspiration"

	GIT_INDEX_FILE="$index_file" git -C "$repo_root" read-tree HEAD
	make_blob="$(git -C "$repo_root" hash-object -w "$makefile")"
	inspiration_blob="$(git -C "$repo_root" hash-object -w "$inspiration")"
	GIT_INDEX_FILE="$index_file" git -C "$repo_root" update-index --add --cacheinfo "100644,$make_blob,Makefile"
	GIT_INDEX_FILE="$index_file" git -C "$repo_root" update-index --add --cacheinfo "100644,$inspiration_blob,INSPIRATION.md"
	for path in "${feature_paths[@]}"; do
		test -f "$repo_root/$path"
	done
	GIT_INDEX_FILE="$index_file" git -C "$repo_root" add -- "${feature_paths[@]}"
}

show_staged() {
	printf '%s\n' 'Isolated feature change:'
	GIT_INDEX_FILE="$index_file" git -C "$repo_root" diff --cached --name-status
	GIT_INDEX_FILE="$index_file" git -C "$repo_root" diff --cached --stat
}

case "$mode" in
	preview)
		prepare_index
		show_staged
		;;
	commit)
		prepare_index
		if GIT_INDEX_FILE="$index_file" git -C "$repo_root" diff --cached --quiet; then
			printf '%s\n' 'No typed-table-monotone changes to commit.' >&2
			exit 1
		fi
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" commit -m 'feat(sql): add monotone aggregate fast path'
		;;
	push)
		git -C "$repo_root" push origin HEAD:master
		;;
	*)
		printf 'usage: %s {preview|commit|push}\n' "$0" >&2
		exit 2
		;;
esac
