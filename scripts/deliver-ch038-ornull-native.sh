#!/usr/bin/env bash
set -euo pipefail

mode="${1:-deliver}"
if [[ "$mode" != "deliver" ]]; then
	printf '%s\n' 'usage: deliver-ch038-ornull-native.sh deliver' >&2
	exit 2
fi

marker='# CH038_NATIVE_DATAFLOW_FEATURE_TARGETS_BEGIN'
end_marker='# CH038_NATIVE_DATAFLOW_FEATURE_TARGETS_END'
if ! rg -q "$marker" Makefile; then
	printf '%s\n' 'feature Makefile target block is missing' >&2
	exit 1
fi

staged_makefile="/tmp/hatrie-ch038-ornull-native-Makefile"
trap 'rm -f "$staged_makefile"' EXIT
git show HEAD:Makefile > "$staged_makefile"
awk -v marker="$marker" -v end_marker="$end_marker" '
$0 == marker { in_block = 1 }
in_block { print }
$0 == end_marker { exit }
' Makefile >> "$staged_makefile"
mv "$staged_makefile" Makefile

git add Makefile ENGINE_IDEAS.md CH038_OR_NULL_NATIVE_DATAFLOW.md \
	hat/hatSql/m052c_native_dataflow.go \
	hat/hatSql/ch038_ornull_native_test.go \
	scripts/benchmark-ch038-ornull-native.sh \
	scripts/deliver-ch038-ornull-native.sh \
	scripts/format-ch038-ornull-native.sh \
	scripts/race-ch038-ornull-native.sh \
	scripts/test-ch038-ornull-native-package.sh \
	scripts/test-ch038-ornull-native.sh \
	scripts/vet-ch038-ornull-native.sh
git diff --cached --check
git diff --cached --name-only
git commit -m 'feat: accelerate native OrNull aggregates [skip ci]'
git push origin HEAD
