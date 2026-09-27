#!/usr/bin/env bash
set -euo pipefail

mode=${1:-status}
commit_message='feat: add opt-in compaction coalescing [skip ci]'
feature_files=(
	ENGINE_IDEAS.md
	MZ004_COMPACTION_POLICY.md
	MZ004_COMPACTION_PRIORITY.md
	MZ004_COMPACTION_COALESCING.md
	hat/hatPipeline/compaction_coalescing.go
	hat/hatPipeline/frontier_compaction_scheduler.go
	hat/hatPipeline/mz004_compaction_coalescing_benchmark_test.go
	hat/hatPipeline/mz004_compaction_coalescing_test.go
	scripts/benchmark-mz004-compaction-coalescing.sh
	scripts/format-mz004-compaction-coalescing.sh
	scripts/test-mz004-compaction-coalescing-package.sh
	scripts/test-mz004-compaction-coalescing.sh
	scripts/race-mz004-compaction-coalescing.sh
	scripts/vet-mz004-compaction-coalescing.sh
	scripts/deliver-mz004-compaction-coalescing.sh
)

case "$mode" in
	status|stage|commit|push|deliver)
		;;
	*)
		echo "usage: $0 {status|stage|commit|push|deliver}" >&2
		exit 2
		;;
esac

if [[ "$mode" == status ]]; then
	echo 'Feature worktree status:'
git status --short --untracked-files=all
echo 'Feature-path diff summary:'
git diff --stat -- "${feature_files[@]}" Makefile
exit 0
fi

stage_feature() {
	if ! git diff --cached --quiet; then
		echo 'Refusing to stage: the index already contains unrelated staged changes.' >&2
		exit 1
	fi

	for path in "${feature_files[@]}"; do
		git add -- "$path"
	done

	temp_dir=$(mktemp -d "${TMPDIR:-/tmp}/hatrie-mz004-coalescing-delivery.XXXXXX")
	base_makefile="$temp_dir/base-Makefile"
	candidate_makefile="$temp_dir/candidate-Makefile"
	block_file="$temp_dir/feature-block"
	patch_file="$temp_dir/Makefile.patch"

	git show HEAD:Makefile > "$base_makefile"
	awk '
		/^# MZ004_COMPACTION_COALESCING_TARGETS_BEGIN$/ { capture=1 }
		capture { print }
		/^# MZ004_COMPACTION_COALESCING_TARGETS_END$/ { capture=0; found=1 }
		END { if (!found) exit 1 }
	' Makefile > "$block_file"
	awk -v block="$block_file" '
		{ print }
		/^# MZ004_COMPACTION_PRIORITY_TARGETS_END$/ {
			while ((getline line < block) > 0) print line
			close(block)
		}
	' "$base_makefile" > "$candidate_makefile"

	set +e
	diff -u -L a/Makefile -L b/Makefile "$base_makefile" "$candidate_makefile" > "$patch_file"
	diff_status=$?
	set -e
	if [[ "$diff_status" -ne 1 ]]; then
		echo "unexpected Makefile patch status: $diff_status" >&2
		exit 1
	fi
	git apply --cached "$patch_file"
	rm -rf -- "$temp_dir"

	git diff --cached --check
	echo 'Staged feature paths:'
	git diff --cached --name-status
}

verify_staged_feature() {
	for path in Makefile "${feature_files[@]}"; do
		if git diff --cached --quiet -- "$path"; then
			echo "expected staged feature path is missing: $path" >&2
			exit 1
		fi
	done
	while IFS= read -r path; do
		case "$path" in
			Makefile)
				;;
			ENGINE_IDEAS.md|MZ004_COMPACTION_POLICY.md|MZ004_COMPACTION_PRIORITY.md|MZ004_COMPACTION_COALESCING.md)
				;;
			hat/hatPipeline/compaction_coalescing.go|hat/hatPipeline/frontier_compaction_scheduler.go|hat/hatPipeline/mz004_compaction_coalescing_benchmark_test.go|hat/hatPipeline/mz004_compaction_coalescing_test.go)
				;;
			scripts/benchmark-mz004-compaction-coalescing.sh|scripts/deliver-mz004-compaction-coalescing.sh|scripts/format-mz004-compaction-coalescing.sh|scripts/race-mz004-compaction-coalescing.sh|scripts/test-mz004-compaction-coalescing-package.sh|scripts/test-mz004-compaction-coalescing.sh|scripts/vet-mz004-compaction-coalescing.sh)
				;;
			*)
				echo "unexpected staged path: $path" >&2
				exit 1
				;;
		esac
	done < <(git diff --cached --name-only)
	git diff --cached --check
}

case "$mode" in
	stage)
		stage_feature
		;;
	commit)
		if git diff --cached --quiet; then
			stage_feature
		else
			verify_staged_feature
		fi
		git commit -m "$commit_message"
		;;
	push)
		git push origin HEAD
		;;
	deliver)
		stage_feature
		git commit -m "$commit_message"
		git push origin HEAD
		;;
esac
