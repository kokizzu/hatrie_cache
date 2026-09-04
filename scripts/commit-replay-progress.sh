#!/usr/bin/env bash
set -euo pipefail

mode="${1:-commit}"
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

feature_files=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	INSPIRATION.md
	README.md
	REPLAY_PROGRESS.md
	hat/hatCache/journal.go
	hat/hatCache/journal_replay_progress.go
	hat/hatCache/journal_replay_progress_benchmark_test.go
	hat/hatCache/journal_replay_progress_test.go
	journal_replay_progress_api.go
	scripts/benchmark-replay-progress.sh
	scripts/commit-replay-progress.sh
	scripts/format-replay-progress.sh
	scripts/review-replay-progress.sh
	scripts/test-replay-progress.sh
	scripts/test-race-replay-progress.sh
	scripts/vet-replay-progress.sh
)

expected_files=(Makefile "${feature_files[@]}")

verify_staged() {
	mapfile -t staged_files < <(git diff --cached --name-only | sort)
	mapfile -t sorted_expected < <(printf '%s\n' "${expected_files[@]}" | sort)
	if [[ "${staged_files[*]}" != "${sorted_expected[*]}" ]]; then
		echo "unexpected staged paths" >&2
		printf 'staged: %s\n' "${staged_files[*]}" >&2
		printf 'expected: %s\n' "${sorted_expected[*]}" >&2
		git reset -- "${expected_files[@]}"
		exit 1
	fi
	git diff --cached --check
}

stage_feature() {
	if [[ -n "$(git diff --cached --name-only)" ]]; then
		echo "refusing to stage: index already contains changes" >&2
		exit 1
	fi

	git add -- "${feature_files[@]}"

	base="$tmpdir/Makefile.base"
	desired="$tmpdir/Makefile.desired"
	patch_file="$tmpdir/Makefile.patch"
	git show HEAD:Makefile > "$base"
	cp "$base" "$desired"
	cat >> "$desired" <<'EOF'

.PHONY: test-replay-progress
test-replay-progress:
	bash ./scripts/test-replay-progress.sh

.PHONY: format-replay-progress
format-replay-progress:
	bash ./scripts/format-replay-progress.sh

.PHONY: benchmark-replay-progress
benchmark-replay-progress:
	bash ./scripts/benchmark-replay-progress.sh

.PHONY: test-race-replay-progress
test-race-replay-progress:
	bash ./scripts/test-race-replay-progress.sh

.PHONY: vet-replay-progress
vet-replay-progress:
	bash ./scripts/vet-replay-progress.sh

.PHONY: review-replay-progress
review-replay-progress:
	bash ./scripts/review-replay-progress.sh

.PHONY: stage-replay-progress
stage-replay-progress:
	bash ./scripts/commit-replay-progress.sh stage

.PHONY: commit-replay-progress
commit-replay-progress:
	bash ./scripts/commit-replay-progress.sh commit

.PHONY: push-replay-progress
push-replay-progress:
	bash ./scripts/commit-replay-progress.sh push
EOF

	diff_status=0
	diff -u --label a/Makefile --label b/Makefile "$base" "$desired" > "$patch_file" || diff_status=$?
	if [[ "$diff_status" -ne 0 && "$diff_status" -ne 1 ]]; then
		exit "$diff_status"
	fi
	git apply --cached "$patch_file"
	verify_staged
}

case "$mode" in
stage)
	stage_feature
	;;
commit)
	stage_feature
	git commit -m 'ops: add journal replay progress'
	;;
push)
	git push origin HEAD
	;;
*)
	echo "usage: $0 {stage|commit|push}" >&2
	exit 2
	;;
esac
