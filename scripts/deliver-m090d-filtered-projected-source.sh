#!/usr/bin/env bash
set -euo pipefail

mode="${1:-deliver}"
if [[ "$mode" != "deliver" ]]; then
	printf '%s\n' 'usage: deliver-m090d-filtered-projected-source.sh deliver' >&2
	exit 2
fi

marker='# M090D_FILTERED_PROJECTED_SOURCE_TARGETS_BEGIN'
end_marker='# M090D_FILTERED_PROJECTED_SOURCE_TARGETS_END'
if ! rg -q "$marker" Makefile; then
	printf '%s\n' 'feature Makefile target block is missing' >&2
	exit 1
fi

staged_makefile="/tmp/hatrie-m090d-filtered-projected-source-Makefile"
normalized_makefile="${staged_makefile}.normalized"
trap 'rm -f "$staged_makefile" "$normalized_makefile"' EXIT
git show HEAD:Makefile > "$staged_makefile"
if ! rg -q "$marker" "$staged_makefile"; then
	awk -v marker="$marker" -v end_marker="$end_marker" '
	$0 == marker { in_block = 1 }
	in_block { print }
	$0 == end_marker { exit }
	' Makefile >> "$staged_makefile"
fi
awk '{
	if (substr($0, 1, 2) == "\\t") {
		sub(/^\\t/, "\t")
	}
	print
}' "$staged_makefile" > "$normalized_makefile"
mv "$normalized_makefile" "$staged_makefile"
mv "$staged_makefile" Makefile

git add Makefile M090D_FILTERED_PROJECTED_SOURCE.md \
	hat/hatSql/catalog.go \
	hat/hatSql/contracts.go \
	hat/hatSql/m090c_projected_source.go \
	hat/hatSql/m090d_filtered_projected_source.go \
	hat/hatSql/m090d_filtered_projected_source_test.go \
	hat/hatSql/query.go \
	hat/hatSql/session.go \
	scripts/benchmark-m090d-filtered-projected-source.sh \
	scripts/deliver-m090d-filtered-projected-source.sh \
	scripts/format-m090d-filtered-projected-source.sh \
	scripts/race-m090d-filtered-projected-source.sh \
	scripts/test-m090d-filtered-projected-source-package.sh \
	scripts/test-m090d-filtered-projected-source.sh \
	scripts/vet-m090d-filtered-projected-source.sh
git diff --cached --check
git diff --cached --name-only
git commit -m 'feat: add filtered projected SQL sources [skip ci]'
git push origin HEAD
