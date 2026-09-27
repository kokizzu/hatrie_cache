#!/usr/bin/env bash
set -euo pipefail

mode="${1:-status}"
commit_message="feat: add opt-in compaction priority scheduling [skip ci]"
begin_marker="# MZ004_COMPACTION_PRIORITY_TARGETS_BEGIN"
end_marker="# MZ004_COMPACTION_PRIORITY_TARGETS_END"
feature_files=(
    ENGINE_IDEAS.md
    MZ004_COMPACTION_POLICY.md
    MZ004_COMPACTION_PRIORITY.md
    hat/hatPipeline/frontier_compaction_scheduler.go
    hat/hatPipeline/mz004_compaction_priority_benchmark_test.go
    hat/hatPipeline/mz004_compaction_priority_test.go
    hat/hatPipeline/priority_scheduler.go
    scripts/benchmark-mz004-compaction-priority.sh
    scripts/format-mz004-compaction-priority.sh
    scripts/race-mz004-compaction-priority.sh
    scripts/test-mz004-compaction-priority-package.sh
    scripts/test-mz004-compaction-priority.sh
    scripts/vet-mz004-compaction-priority.sh
    scripts/deliver-mz004-compaction-priority.sh
)

status() {
    git status --short
    printf 'Staged paths:\n'
    git diff --cached --name-status
}

stage_makefile_block() {
    local temp_dir block_file base_file desired_file patch_file
    temp_dir="$(mktemp -d /tmp/hatrie-mz004-delivery.XXXXXX)"
    block_file="$temp_dir/block"
    base_file="$temp_dir/base"
    desired_file="$temp_dir/desired"
    patch_file="$temp_dir/Makefile.patch"

    [[ "$(grep -c "^${begin_marker}$" Makefile)" -eq 1 ]] || {
        printf 'Expected exactly one MZ004 Makefile begin marker.\n' >&2
        exit 1
    }
    [[ "$(grep -c "^${end_marker}$" Makefile)" -eq 1 ]] || {
        printf 'Expected exactly one MZ004 Makefile end marker.\n' >&2
        exit 1
    }
    if git show HEAD:Makefile | grep -q "^${begin_marker}$"; then
        printf 'MZ004 Makefile block already exists in HEAD.\n' >&2
        exit 1
    fi

    awk -v begin="$begin_marker" -v end="$end_marker" '
        $0 == begin { found++; inside = 1 }
        inside { print }
        inside && $0 == end { inside = 0 }
        END { if (found != 1 || inside) exit 1 }
    ' Makefile > "$block_file"
    git show HEAD:Makefile > "$base_file"
    awk -v block_path="$block_file" '
        BEGIN {
            while ((getline line < block_path) > 0) block = block line ORS
            close(block_path)
        }
        {
            print
            if (!inserted && $0 == "# TT024_TEXT_INTERSECTION_TARGETS_END") {
                printf "%s", block
                inserted = 1
            }
        }
        END { if (!inserted) exit 1 }
    ' "$base_file" > "$desired_file"

    if diff -u --label a/Makefile --label b/Makefile "$base_file" "$desired_file" > "$patch_file"; then
        printf 'MZ004 Makefile block produced no diff.\n' >&2
        exit 1
    else
        diff_status=$?
        [[ "$diff_status" -eq 1 ]] || exit "$diff_status"
    fi
    git apply --cached "$patch_file"
    rm -rf -- "$temp_dir"
}

stage() {
    if ! git diff --cached --quiet; then
        printf 'Refusing to stage over pre-existing staged changes:\n' >&2
        git diff --cached --name-status >&2
        exit 1
    fi
    git add -- "${feature_files[@]}"
    stage_makefile_block
    git diff --cached --check
    printf 'Staged MZ004 compaction priority paths:\n'
    git diff --cached --name-status
}

commit() {
    git diff --cached --quiet && {
        printf 'No staged MZ004 changes to commit.\n' >&2
        exit 1
    }
    git diff --cached --check
    git commit -m "$commit_message"
}

push() {
    git push origin HEAD
}

unstage() {
    git restore --staged -- Makefile "${feature_files[@]}"
}

case "$mode" in
status)
    status
    ;;
stage)
    stage
    ;;
commit)
    commit
    ;;
push)
    push
    ;;
unstage)
    unstage
    ;;
deliver)
    stage
    commit
    push
    ;;
*)
    printf 'usage: %s [status|stage|commit|push|deliver]\n' "$0" >&2
    exit 2
    ;;
esac
