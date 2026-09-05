#!/usr/bin/env bash
set -euo pipefail

if git diff --cached --quiet -- Makefile; then
	line_count="$(git show HEAD:Makefile | wc -l)"
	line_count="${line_count//[[:space:]]/}"
	next_line=$((line_count + 1))
	patch_file="$(mktemp)"
	trap 'rm -f "$patch_file"' EXIT
	{
		printf '%s\n' 'diff --git a/Makefile b/Makefile'
		printf '%s\n' '--- a/Makefile' '+++ b/Makefile'
		printf '@@ -%s,0 +%s,36 @@\n' "$line_count" "$next_line"
		printf '%s\n' '+'
		printf '%s\n' '+.PHONY: test-t041' '+test-t041:' '+\tbash ./scripts/test-t041.sh' '+'
		printf '%s\n' '+.PHONY: format-t041' '+format-t041:' '+\tbash ./scripts/format-t041.sh' '+'
		printf '%s\n' '+.PHONY: benchmark-t041' '+benchmark-t041:' '+\tbash ./scripts/benchmark-t041.sh' '+'
		printf '%s\n' '+.PHONY: test-race-t041' '+test-race-t041:' '+\tbash ./scripts/test-race-t041.sh' '+'
		printf '%s\n' '+.PHONY: vet-t041' '+vet-t041:' '+\tbash ./scripts/vet-t041.sh' '+'
		printf '%s\n' '+.PHONY: review-t041' '+review-t041:' '+\tbash ./scripts/review-t041.sh' '+'
		printf '%s\n' '+.PHONY: stage-t041' '+stage-t041:' '+\tbash ./scripts/stage-t041.sh' '+'
		printf '%s\n' '+.PHONY: commit-t041' '+commit-t041:' '+\tbash ./scripts/commit-t041.sh' '+'
		printf '%s\n' '+.PHONY: push-t041' '+push-t041:' '+\tbash ./scripts/push-t041.sh'
	} > "$patch_file"
	git apply --cached "$patch_file"
else
	staged_makefile="$(git diff --cached -- Makefile)"
	if [[ "$staged_makefile" != *"+.PHONY: test-t041"* || "$staged_makefile" != *"+.PHONY: push-t041"* ]]; then
		echo "Makefile has unrelated staged changes; refusing to mix concurrent work" >&2
		exit 1
	fi
fi

git add \
	README.md \
	INSPIRATION.md \
	api_journal_compression.go \
	cmd/hatrie-cache/main.go \
	cmd/hatrie-cache/main_test.go \
	go.mod \
	hat/hatCache/journal.go \
	hat/hatCache/journal_segments.go \
	hat/hatCache/journal_zstd_backup_test.go \
	hat/hatCache/journal_zstd_segment_benchmark_test.go \
	hat/hatCache/journal_zstd_segment_test.go \
	hat/hatJournal/compress.go \
	hat/hatJournal/journal.go \
	hat/hatJournal/reader.go \
	scripts/benchmark-t041.sh \
	scripts/commit-t041.sh \
	scripts/format-t041.sh \
	scripts/push-t041.sh \
	scripts/review-t041.sh \
	scripts/stage-t041.sh \
	scripts/test-race-t041.sh \
	scripts/test-t041.sh \
	scripts/vet-t041.sh

git diff --cached --check
