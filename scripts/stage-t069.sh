#!/usr/bin/env bash
set -euo pipefail

if git show HEAD:Makefile | rg -q '^test-t069:'; then
	:
elif git diff --cached --quiet -- Makefile; then
	line_count="$(git show HEAD:Makefile | wc -l)"
	line_count="${line_count//[[:space:]]/}"
	next_line=$((line_count + 1))
	patch_file="$(mktemp)"
	trap 'rm -f "$patch_file"' EXIT
	{
		printf '%s\n' 'diff --git a/Makefile b/Makefile'
		printf '%s\n' '--- a/Makefile' '+++ b/Makefile'
		printf '@@ -%s,0 +%s,32 @@\n' "$line_count" "$next_line"
		printf '%s\n' '+'
		printf '%s\n' '+.PHONY: test-t069' '+test-t069:' '+\tbash ./scripts/test-t069.sh' '+'
		printf '%s\n' '+.PHONY: format-t069' '+format-t069:' '+\tbash ./scripts/format-t069.sh' '+'
		printf '%s\n' '+.PHONY: test-race-t069' '+test-race-t069:' '+\tbash ./scripts/test-race-t069.sh' '+'
		printf '%s\n' '+.PHONY: vet-t069' '+vet-t069:' '+\tbash ./scripts/vet-t069.sh' '+'
		printf '%s\n' '+.PHONY: review-t069' '+review-t069:' '+\tbash ./scripts/review-t069.sh' '+'
		printf '%s\n' '+.PHONY: stage-t069' '+stage-t069:' '+\tbash ./scripts/stage-t069.sh' '+'
		printf '%s\n' '+.PHONY: commit-t069' '+commit-t069:' '+\tbash ./scripts/commit-t069.sh' '+'
		printf '%s\n' '+.PHONY: push-t069' '+push-t069:' '+\tbash ./scripts/push-t069.sh'
	} > "$patch_file"
	git apply --cached "$patch_file"
else
	staged_makefile="$(git diff --cached -- Makefile)"
	if [[ "$staged_makefile" != *"+.PHONY: test-t069"* || "$staged_makefile" != *"+.PHONY: push-t069"* ]]; then
		echo "Makefile has unrelated staged changes; refusing to mix concurrent work" >&2
		exit 1
	fi
fi

git add \
	INSPIRATION.md \
	README.md \
	hat/hatBackup/reports.go \
	hat/hatCache/backup_checksum.go \
	hat/hatCache/backup_doctor.go \
	hat/hatCache/backup_rehearsal_checksum_test.go \
	hat/hatCache/backup_restore.go \
	scripts/commit-t069.sh \
	scripts/format-t069.sh \
	scripts/push-t069.sh \
	scripts/review-t069.sh \
	scripts/stage-t069.sh \
	scripts/test-race-t069.sh \
	scripts/test-t069.sh \
	scripts/vet-t069.sh

git diff --cached --check
