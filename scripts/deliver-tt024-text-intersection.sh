#!/usr/bin/env bash
set -euo pipefail

mode=${1:-status}
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$root"

feature_files=(
	TT024_MULTI_FIELD_TEXT_INTERSECTION.md
	hat/hatSql/contracts.go
	hat/hatSql/query.go
	hat/hatSql/text_proximity.go
	hat/hatCache/monitoring.go
	hat/hatCache/sql_text_phrase.go
	hat/hatCache/sql_text_phrase_test.go
	hat/hatCache/tt024_text_intersection_benchmark_test.go
	hat/hatSchema/text_index_resolver.go
	scripts/format-tt024-text-intersection.sh
	scripts/test-tt024-text-intersection.sh
	scripts/test-tt024-text-intersection-package.sh
	scripts/race-tt024-text-intersection.sh
	scripts/vet-tt024-text-intersection.sh
	scripts/benchmark-tt024-text-intersection.sh
	scripts/deliver-tt024-text-intersection.sh
	Makefile
)

case "$mode" in
status)
	git status --short -- "${feature_files[@]}"
	;;
deliver)
	mapfile -t staged_files < <(git diff --cached --name-only)
	if ((${#staged_files[@]} > 0)); then
		for path in "${staged_files[@]}"; do
			allowed=false
			for feature_path in "${feature_files[@]}"; do
			if [[ "$path" == "$feature_path" ]]; then
				allowed=true
				break
			fi
			done
			if [[ "$allowed" != true ]]; then
				echo "refusing to commit unrelated staged path: $path" >&2
				exit 1
			fi
		done
	fi
	if ! git diff --cached --quiet -- "${feature_files[@]}"; then
		echo "refusing to overwrite existing staged TT024 changes" >&2
		exit 1
	fi

	git add -- \
		TT024_MULTI_FIELD_TEXT_INTERSECTION.md \
		hat/hatSql/contracts.go \
		hat/hatSql/query.go \
		hat/hatSql/text_proximity.go \
		hat/hatCache/monitoring.go \
		hat/hatCache/sql_text_phrase.go \
		hat/hatCache/sql_text_phrase_test.go \
		hat/hatCache/tt024_text_intersection_benchmark_test.go \
		hat/hatSchema/text_index_resolver.go \
		scripts/format-tt024-text-intersection.sh \
		scripts/test-tt024-text-intersection.sh \
		scripts/test-tt024-text-intersection-package.sh \
		scripts/race-tt024-text-intersection.sh \
		scripts/vet-tt024-text-intersection.sh \
		scripts/benchmark-tt024-text-intersection.sh \
		scripts/deliver-tt024-text-intersection.sh

	tmp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-cache-tt024-stage.XXXXXX")
	trap 'rm -rf "$tmp_dir"' EXIT
	base_file="$tmp_dir/base.Makefile"
	feature_file="$tmp_dir/feature.Makefile"
	block_file="$tmp_dir/tt024.block"
	patch_file="$tmp_dir/tt024.patch"
	git show HEAD:Makefile > "$base_file"
	sed -n '/^# TT024_TEXT_INTERSECTION_TARGETS_BEGIN$/,/^# TT024_TEXT_INTERSECTION_TARGETS_END$/p' Makefile > "$block_file"
	if ! grep -Fq '# TT024_TEXT_INTERSECTION_TARGETS_BEGIN' "$block_file"; then
		echo "TT024 Makefile target block is missing" >&2
		exit 1
	fi
	if ! grep -Fq '# CH037_NATIVE_ARRAY_FASTPATH_TARGETS_END' "$base_file"; then
		echo "CH037 Makefile insertion point is missing from HEAD" >&2
		exit 1
	fi
	awk -v block_file="$block_file" '
		function print_block(line) {
			while ((getline line < block_file) > 0) print line
			close(block_file)
		}
		$0 == "# TT024_TEXT_INTERSECTION_TARGETS_BEGIN" {
			if (!replaced) {
				print_block()
				replaced=1
			}
			in_block=1
			next
		}
		in_block && $0 == "# TT024_TEXT_INTERSECTION_TARGETS_END" {
			in_block=0
			next
		}
		in_block { next }
		$0 == "# CH037_NATIVE_ARRAY_FASTPATH_TARGETS_END" && !replaced {
			print
			print_block()
			replaced=1
			next
		}
		{ print }
		END {
			if (!replaced) exit 2
		}
	' "$base_file" > "$feature_file"
	diff_status=0
	diff -u "$base_file" "$feature_file" > "$patch_file" || diff_status=$?
	if [[ "$diff_status" -ne 1 ]]; then
		echo "could not construct the isolated Makefile patch" >&2
		exit 1
	fi
	sed -i '1c\\--- a/Makefile' "$patch_file"
	sed -i '2c\\+++ b/Makefile' "$patch_file"
	git apply --cached "$patch_file"

	git diff --cached --check
	staged_feature=false
	for path in "${feature_files[@]}"; do
		if ! git diff --cached --quiet -- "$path"; then
			staged_feature=true
			break
		fi
	done
	if [[ "$staged_feature" != true ]]; then
		echo "no intended TT024 changes are staged" >&2
		exit 1
	fi
	git commit -m 'perf: intersect multi-field text indexes [skip ci]'
	git push origin HEAD
	;;
*)
	echo "usage: $0 [status|deliver]" >&2
	exit 2
	;;
esac
