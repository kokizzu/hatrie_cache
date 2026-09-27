#!/usr/bin/env bash
set -euo pipefail

root_dir=$(cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root_dir"

if [[ "${1:-}" != "deliver" ]]; then
	printf 'usage: %s deliver\n' "$0" >&2
	exit 2
fi

if ! git diff --cached --quiet; then
	printf 'refusing delivery: the index already contains staged changes\n' >&2
	exit 1
fi

marker='# CH037_NATIVE_ARRAY_FASTPATH_TARGETS_BEGIN'
if ! grep -Fqx "$marker" Makefile; then
	printf 'refusing delivery: CH037 Makefile target block is missing\n' >&2
	exit 1
fi

expected=$(mktemp)
block=$(mktemp)
cleanup() {
	rm -f "$expected" "$block"
}
trap cleanup EXIT

printf '%s\n' \
	'' \
	'# CH037_NATIVE_ARRAY_FASTPATH_TARGETS_BEGIN' \
	'.PHONY: test-ch037-native-array-fastpath' \
	'test-ch037-native-array-fastpath:' \
	$'\tbash ./scripts/test-ch037-native-array-fastpath.sh' \
	'.PHONY: benchmark-ch037-native-array-fastpath' \
	'benchmark-ch037-native-array-fastpath:' \
	$'\tbash ./scripts/benchmark-ch037-native-array-fastpath.sh' \
	'.PHONY: format-ch037-native-array-fastpath' \
	'format-ch037-native-array-fastpath:' \
	$'\tbash ./scripts/format-ch037-native-array-fastpath.sh' \
	'.PHONY: test-ch037-native-array-fastpath-package' \
	'test-ch037-native-array-fastpath-package:' \
	$'\tbash ./scripts/test-ch037-native-array-fastpath-package.sh' \
	'.PHONY: race-ch037-native-array-fastpath' \
	'race-ch037-native-array-fastpath:' \
	$'\tbash ./scripts/race-ch037-native-array-fastpath.sh' \
	'.PHONY: vet-ch037-native-array-fastpath' \
	'vet-ch037-native-array-fastpath:' \
	$'\tbash ./scripts/vet-ch037-native-array-fastpath.sh' \
	'.PHONY: deliver-ch037-native-array-fastpath' \
	'deliver-ch037-native-array-fastpath:' \
	$'\tbash ./scripts/deliver-ch037-native-array-fastpath.sh deliver' \
	'# CH037_NATIVE_ARRAY_FASTPATH_TARGETS_END' >"$block"

git show HEAD:Makefile >"$expected"
if ! grep -Fqx "$marker" "$expected"; then
	awk -v block="$block" '
		{ print }
		/# M090E_PREDICATE_COLUMNAR_SOURCE_TARGETS_END/ {
			while ((getline line < block) > 0) print line
			close(block)
		}
	' "$expected" >"$expected.next"
	mv "$expected.next" "$expected"
fi
if ! cmp -s "$expected" Makefile; then
	printf 'refusing delivery: Makefile has changes outside the CH037 block\n' >&2
	diff -u "$expected" Makefile || true
	exit 1
fi

git add \
	CH037_NATIVE_ARRAY_FASTPATH.md \
	Makefile \
	hat/hatSql/ch037_columnar_array_join_test.go \
	hat/hatSql/columnar_array_join.go \
	scripts/benchmark-ch037-native-array-fastpath.sh \
	scripts/deliver-ch037-native-array-fastpath.sh \
	scripts/format-ch037-native-array-fastpath.sh \
	scripts/race-ch037-native-array-fastpath.sh \
	scripts/test-ch037-native-array-fastpath-package.sh \
	scripts/test-ch037-native-array-fastpath.sh \
	scripts/vet-ch037-native-array-fastpath.sh
git diff --cached --check
printf '%s\n' '--- staged CH037 files ---'
git diff --cached --name-only
git commit -m 'perf: add native columnar array join fast path [skip ci]'
git push origin HEAD
