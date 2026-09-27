#!/usr/bin/env bash
set -euo pipefail

mode="${1:-deliver}"
paths=(
	TT024_MIXED_BOOLEAN_TEXT_INDEX.md
	hat/hatSql/query.go
	hat/hatSql/text_phrase_test.go
	hat/hatSql/text_proximity.go
	hat/hatSql/tt024_mixed_boolean_benchmark_test.go
	scripts/benchmark-tt024-text.sh
	scripts/cleanup-tt024-tmp.sh
	scripts/format-tt024-text.sh
	scripts/race-tt024-text.sh
	scripts/test-tt024-package.sh
	scripts/test-tt024-text.sh
	scripts/vet-tt024-text.sh
	scripts/deliver-tt024-mixed-boolean.sh
)

makefile_block=$(mktemp "${TMPDIR:-/tmp}/hatrie-tt024-makefile-block.XXXXXX")
base_makefile=$(mktemp "${TMPDIR:-/tmp}/hatrie-tt024-makefile-base.XXXXXX")
desired_makefile=$(mktemp "${TMPDIR:-/tmp}/hatrie-tt024-makefile-desired.XXXXXX")
makefile_patch=$(mktemp "${TMPDIR:-/tmp}/hatrie-tt024-makefile.patch.XXXXXX")
trap 'rm -f "$makefile_block" "$base_makefile" "$desired_makefile" "$makefile_patch"' EXIT

printf '%s\n' \
	'.PHONY: test-tt024-text' \
	'test-tt024-text:' \
	'\tbash ./scripts/test-tt024-text.sh' \
	'.PHONY: cleanup-tt024-tmp-preview cleanup-tt024-tmp' \
	'cleanup-tt024-tmp-preview:' \
	'\tbash ./scripts/cleanup-tt024-tmp.sh preview' \
	'cleanup-tt024-tmp:' \
	'\tbash ./scripts/cleanup-tt024-tmp.sh apply' \
	'.PHONY: format-tt024-text' \
	'format-tt024-text:' \
	'\tbash ./scripts/format-tt024-text.sh' \
	'.PHONY: benchmark-tt024-text' \
	'benchmark-tt024-text:' \
	'\tbash ./scripts/benchmark-tt024-text.sh' \
	'.PHONY: race-tt024-text vet-tt024-text test-tt024-package' \
	'race-tt024-text:' \
	'\tbash ./scripts/race-tt024-text.sh' \
	'vet-tt024-text:' \
	'\tbash ./scripts/vet-tt024-text.sh' \
	'test-tt024-package:' \
	'\tbash ./scripts/test-tt024-package.sh' \
	'.PHONY: deliver-tt024-mixed-boolean' \
	'deliver-tt024-mixed-boolean:' \
	'\tbash ./scripts/deliver-tt024-mixed-boolean.sh deliver' >"$makefile_block"

case "$mode" in
stage|commit|push|deliver) ;;
*)
	printf 'usage: %s [stage|commit|push|deliver]\n' "$0" >&2
	exit 2
;;
esac

if ! git diff --cached --quiet --; then
	printf '%s\n' 'Refusing to stage TT-024: the index already contains unrelated staged changes.' >&2
	exit 1
fi

if [ "$mode" = stage ] || [ "$mode" = commit ] || [ "$mode" = deliver ]; then
	git add -- "${paths[@]}"
	git show HEAD:Makefile >"$base_makefile"
	cp "$base_makefile" "$desired_makefile"
	printf '\n' >>"$desired_makefile"
	cat "$makefile_block" >>"$desired_makefile"
	if diff -u "$base_makefile" "$desired_makefile" >"$makefile_patch"; then
		:
	else
		:
	fi
	sed -i '1s|^--- .*|--- a/Makefile|; 2s|^+++ .*|+++ b/Makefile|' "$makefile_patch"
	git apply --cached --unidiff-zero "$makefile_patch"
fi

if [ "$mode" = commit ] || [ "$mode" = deliver ]; then
	git commit -m 'feat: add TT-024 mixed Boolean text index planning [skip ci]'
fi

if [ "$mode" = push ] || [ "$mode" = deliver ]; then
	git push origin HEAD
fi

printf 'TT-024 delivery mode completed: %s\n' "$mode"
