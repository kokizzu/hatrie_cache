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
	"DIFFERENTIAL_WATERMARK.md"
	"hat/hatSql/differential_watermark.go"
	"hat/hatSql/differential_watermark_test.go"
	"scripts/test-sql-differential-watermark.sh"
	"scripts/test-race-sql-differential-watermark.sh"
	"scripts/format-sql-differential-watermark.sh"
	"scripts/benchmark-sql-differential-watermark.sh"
	"scripts/deliver-sql-differential-watermark.sh"
)

prepare_index() {
	git -C "$repo_root" show HEAD:Makefile > "$makefile"
	if ! grep -Fq 'test-sql-differential-watermark:' "$makefile"; then
		cat >> "$makefile" <<'EOF'

.PHONY: test-sql-differential-watermark
test-sql-differential-watermark:
	bash ./scripts/test-sql-differential-watermark.sh

.PHONY: test-race-sql-differential-watermark
test-race-sql-differential-watermark:
	bash ./scripts/test-race-sql-differential-watermark.sh

.PHONY: format-sql-differential-watermark
format-sql-differential-watermark:
	bash ./scripts/format-sql-differential-watermark.sh

.PHONY: benchmark-sql-differential-watermark
benchmark-sql-differential-watermark:
	bash ./scripts/benchmark-sql-differential-watermark.sh

.PHONY: deliver-sql-differential-watermark
deliver-sql-differential-watermark:
	bash ./scripts/deliver-sql-differential-watermark.sh preview

.PHONY: commit-sql-differential-watermark
commit-sql-differential-watermark:
	bash ./scripts/deliver-sql-differential-watermark.sh commit

.PHONY: push-sql-differential-watermark
push-sql-differential-watermark:
	bash ./scripts/deliver-sql-differential-watermark.sh push
EOF
	fi

	git -C "$repo_root" show HEAD:INSPIRATION.md > "$inspiration"
	awk '
		BEGIN {
			line = "- [x] M072a Monotone differential watermark propagation."
			added = 0
		}
		$0 == line {
			added = 1
			print
			next
		}
		$0 ~ /M072[[:space:]].*[Ww]atermark propagation/ && !added {
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
			printf '%s\n' 'No differential-watermark changes to commit.' >&2
			exit 1
		fi
		GIT_INDEX_FILE="$index_file" git -C "$repo_root" commit -m 'feat(sql): add differential watermark'
		;;
	push)
		git -C "$repo_root" push origin HEAD:master
		;;
	*)
		printf 'usage: %s {preview|commit|push}\n' "$0" >&2
		exit 2
		;;
esac
