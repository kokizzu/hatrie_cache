#!/usr/bin/env bash
set -euo pipefail

mode="${1:-commit}"
root=$(pwd)
tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

feature_files=(
	ADOPTED_QUERY_ENGINE_IDEAS.md
	BENCHMARK.md
	INSPIRATION.md
	JOURNAL_RETENTION.md
	README.md
	cmd/hatrie-cache/main.go
	cmd/hatrie-cache/main_test.go
	deploy/hatrie-cache.json
	hat/hatCache/journal.go
	hat/hatCache/journal_retention_benchmark_test.go
	hat/hatCache/journal_retention_test.go
	hat/hatCache/journal_segments.go
	hat/hatCache/script_defaults_test.go
	hat/hatJournal/journal.go
	hat/hatJournal/journal_retention_test.go
	journal_retention_api.go
	journal_retention_api_test.go
	scripts/benchmark-journal-retention.sh
	scripts/format-journal-retention.sh
	scripts/monitoring-server.sh
	scripts/review-journal-retention.sh
	scripts/test-journal-retention.sh
	scripts/test-race-journal-retention.sh
	scripts/vet-journal-retention.sh
)

expected_files=(Makefile "${feature_files[@]}")

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

	if ! rg -q '^JOURNAL_RETAINED_SEGMENTS \?= 16$' "$desired"; then
		echo "HEAD Makefile is missing the journal retention default anchor" >&2
		exit 1
	fi
	if ! rg -q '^monitoring-server: export JOURNAL_IDEMPOTENCY_CAPACITY := ' "$desired"; then
		echo "HEAD Makefile is missing the monitoring export anchor" >&2
		exit 1
	fi

	sed -i '/^JOURNAL_RETAINED_SEGMENTS ?= 16$/a JOURNAL_RETAINED_BYTES ?= 0' "$desired"
	sed -i '/^monitoring-server: export JOURNAL_IDEMPOTENCY_CAPACITY := $(JOURNAL_IDEMPOTENCY_CAPACITY)$/a monitoring-server: export JOURNAL_RETAINED_BYTES := $(JOURNAL_RETAINED_BYTES)' "$desired"

	cat >> "$desired" <<'EOF'

.PHONY: test-journal-retention
test-journal-retention:
	bash ./scripts/test-journal-retention.sh

.PHONY: format-journal-retention
format-journal-retention:
	bash ./scripts/format-journal-retention.sh

.PHONY: benchmark-journal-retention
benchmark-journal-retention:
	bash ./scripts/benchmark-journal-retention.sh

.PHONY: test-race-journal-retention
test-race-journal-retention:
	bash ./scripts/test-race-journal-retention.sh

.PHONY: vet-journal-retention
vet-journal-retention:
	bash ./scripts/vet-journal-retention.sh

.PHONY: review-journal-retention
review-journal-retention:
	bash ./scripts/review-journal-retention.sh

.PHONY: stage-journal-retention
stage-journal-retention:
	bash ./scripts/commit-journal-retention.sh stage

.PHONY: commit-journal-retention
commit-journal-retention:
	bash ./scripts/commit-journal-retention.sh commit

.PHONY: push-journal-retention
push-journal-retention:
	bash ./scripts/commit-journal-retention.sh push
EOF

	local diff_status=0
	diff -u --label a/Makefile --label b/Makefile "$base" "$desired" > "$patch_file" || diff_status=$?
	if [[ "$diff_status" -ne 0 && "$diff_status" -ne 1 ]]; then
		exit "$diff_status"
	fi
	git apply --cached "$patch_file"

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

stage_wrapper() {
	if [[ -n "$(git diff --cached --name-only)" ]]; then
		echo "refusing to stage: index already contains changes" >&2
		exit 1
	fi
	git add -- scripts/commit-journal-retention.sh
	mapfile -t staged_files < <(git diff --cached --name-only)
	if [[ "${staged_files[*]}" != "scripts/commit-journal-retention.sh" ]]; then
		echo "unexpected staged paths" >&2
		printf 'staged: %s\n' "${staged_files[*]}" >&2
		git reset -- scripts/commit-journal-retention.sh
		exit 1
	fi
	git diff --cached --check
}

case "$mode" in
stage)
	stage_feature
	;;
commit)
	stage_feature
	git commit -m 'ops: add journal byte retention budget'
	;;
push)
	git push origin HEAD
	;;
wrapper-commit)
	stage_wrapper
	git commit -m 'ops: include journal retention delivery wrapper'
	;;
*)
	echo "usage: $0 {stage|commit|push}" >&2
	exit 2
	;;
esac
