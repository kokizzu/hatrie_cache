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

marker='# M090E_PREDICATE_COLUMNAR_SOURCE_TARGETS_BEGIN'
if ! grep -Fqx "$marker" Makefile; then
	printf 'refusing delivery: M090E Makefile target block is missing\n' >&2
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
	'# M090E_PREDICATE_COLUMNAR_SOURCE_TARGETS_BEGIN' \
	'.PHONY: test-m090e-predicate-columnar-source' \
	'test-m090e-predicate-columnar-source:' \
	$'\tbash ./scripts/test-m090e-predicate-columnar-source.sh' \
	'.PHONY: benchmark-m090e-predicate-columnar-source' \
	'benchmark-m090e-predicate-columnar-source:' \
	$'\tbash ./scripts/benchmark-m090e-predicate-columnar-source.sh' \
	'.PHONY: format-m090e-predicate-columnar-source' \
	'format-m090e-predicate-columnar-source:' \
	$'\tbash ./scripts/format-m090e-predicate-columnar-source.sh' \
	'.PHONY: test-m090e-predicate-columnar-source-package' \
	'test-m090e-predicate-columnar-source-package:' \
	$'\tbash ./scripts/test-m090e-predicate-columnar-source-package.sh' \
	'.PHONY: race-m090e-predicate-columnar-source' \
	'race-m090e-predicate-columnar-source:' \
	$'\tbash ./scripts/race-m090e-predicate-columnar-source.sh' \
	'.PHONY: vet-m090e-predicate-columnar-source' \
	'vet-m090e-predicate-columnar-source:' \
	$'\tbash ./scripts/vet-m090e-predicate-columnar-source.sh' \
	'.PHONY: deliver-m090e-predicate-columnar-source' \
	'deliver-m090e-predicate-columnar-source:' \
	$'\tbash ./scripts/deliver-m090e-predicate-columnar-source.sh deliver' \
	'# M090E_PREDICATE_COLUMNAR_SOURCE_TARGETS_END' >"$block"

git show HEAD:Makefile >"$expected"
if ! grep -Fqx "$marker" "$expected"; then
	awk -v block="$block" '
		{ print }
		/# M090D_FILTERED_PROJECTED_SOURCE_TARGETS_END/ {
			while ((getline line < block) > 0) print line
			close(block)
		}
	' "$expected" >"$expected.next"
	mv "$expected.next" "$expected"
fi
if ! cmp -s "$expected" Makefile; then
	printf 'refusing delivery: Makefile has changes outside the M090E block\n' >&2
	diff -u "$expected" Makefile || true
	exit 1
fi

git add \
	M090E_PREDICATE_COLUMNAR_SOURCE.md \
	Makefile \
	hat/hatSql/catalog.go \
	hat/hatSql/ch002_physical_part_pruning.go \
	hat/hatSql/contracts.go \
	hat/hatSql/m090e_predicate_columnar_source.go \
	hat/hatSql/m090e_predicate_columnar_source_test.go \
	hat/hatSql/session.go \
	scripts/benchmark-m090e-predicate-columnar-source.sh \
	scripts/deliver-m090e-predicate-columnar-source.sh \
	scripts/format-m090e-predicate-columnar-source.sh \
	scripts/race-m090e-predicate-columnar-source.sh \
	scripts/test-m090e-predicate-columnar-source-package.sh \
	scripts/test-m090e-predicate-columnar-source.sh \
	scripts/vet-m090e-predicate-columnar-source.sh
git diff --cached --check
printf '%s\n' '--- staged M090E files ---'
git diff --cached --name-only
git commit -m 'feat: add predicate-aware columnar sources [skip ci]'
git push origin HEAD
