#!/usr/bin/env bash
set -euo pipefail

mode="${1:-deliver}"
paths=(
	scripts/cleanup-empty-tmp-plan-artifacts.sh
	scripts/deliver-empty-tmp-plan-cleanup.sh
)
block=$(mktemp "${TMPDIR:-/tmp}/hatrie-empty-plan-makefile-block.XXXXXX")
base=$(mktemp "${TMPDIR:-/tmp}/hatrie-empty-plan-makefile-base.XXXXXX")
desired=$(mktemp "${TMPDIR:-/tmp}/hatrie-empty-plan-makefile-desired.XXXXXX")
patch_file=$(mktemp "${TMPDIR:-/tmp}/hatrie-empty-plan-makefile.patch.XXXXXX")
trap 'rm -f "$block" "$base" "$desired" "$patch_file"' EXIT

case "$mode" in
stage|commit|push|deliver) ;;
*)
	printf 'usage: %s [stage|commit|push|deliver]\n' "$0" >&2
	exit 2
;;
esac

printf '%s\n' \
	'.PHONY: cleanup-empty-tmp-plan-preview cleanup-empty-tmp-plan' \
	'cleanup-empty-tmp-plan-preview:' \
	'\tbash ./scripts/cleanup-empty-tmp-plan-artifacts.sh preview' \
	'cleanup-empty-tmp-plan:' \
	'\tbash ./scripts/cleanup-empty-tmp-plan-artifacts.sh apply' >"$block"

if ! git diff --cached --quiet --; then
	printf '%s\n' 'Refusing to stage cleanup: the index already contains unrelated staged changes.' >&2
	exit 1
fi

if [ "$mode" = stage ] || [ "$mode" = commit ] || [ "$mode" = deliver ]; then
	git add -- "${paths[@]}"
	git show HEAD:Makefile >"$base"
	cp "$base" "$desired"
	printf '\n' >>"$desired"
	cat "$block" >>"$desired"
	if diff -u "$base" "$desired" >"$patch_file"; then
		:
	else
		:
	fi
	sed -i '1s|^--- .*|--- a/Makefile|; 2s|^+++ .*|+++ b/Makefile|' "$patch_file"
	git apply --cached --unidiff-zero "$patch_file"
fi

if [ "$mode" = commit ] || [ "$mode" = deliver ]; then
	git commit -m 'chore: clean empty Hatrie temp plan artifacts [skip ci]'
fi

if [ "$mode" = push ] || [ "$mode" = deliver ]; then
	git push origin HEAD
fi

printf 'Empty-plan cleanup delivery mode completed: %s\n' "$mode"
