#!/usr/bin/env bash
set -euo pipefail

mode="${1:-preview}"
case "$mode" in
preview|commit|push) ;;
*)
	echo "usage: $0 preview|commit|push" >&2
	exit 2
	;;
esac

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

base_commit="$(git rev-parse HEAD)"
branch="$(git symbolic-ref --short HEAD)"
tmpdir="$(mktemp -d "$repo_root/.c015-delivery.XXXXXX")"
trap 'rm -rf -- "$tmpdir"' EXIT
index="$tmpdir/index"
export GIT_INDEX_FILE="$index"
git read-tree "$base_commit"

git show "$base_commit:Makefile" > "$tmpdir/Makefile"
printf '%s\n' \
	'' \
	'# c015-simd-targets' \
	'format-c015:' \
	$'\tbash ./scripts/format-c015.sh' \
	'' \
	'test-c015:' \
	$'\tbash ./scripts/test-c015.sh' \
	'' \
	'race-c015:' \
	$'\tbash ./scripts/race-c015.sh' \
	'' \
	'benchmark-c015:' \
	$'\tbash ./scripts/benchmark-c015.sh' \
	'' \
	'deliver-c015:' \
	$'\tbash ./scripts/deliver-c015.sh preview' \
	'' \
	'commit-c015:' \
	$'\tbash ./scripts/deliver-c015.sh commit' \
	'' \
	'push-c015:' \
	$'\tbash ./scripts/deliver-c015.sh push' \
	>> "$tmpdir/Makefile"

git show "$base_commit:INSPIRATION.md" > "$tmpdir/INSPIRATION.md"
perl -0pi -e '
my $count = s/^- \[ \] C015 SIMD kernels for common numeric and string predicates\.$/- [x] C015 SIMD kernels for common numeric and string predicates. AVX2-gated equality and inequality dispatch is available for typed int64 masks; all other numeric and string predicates retain the portable allocation-free path./m;
die "C015 checklist replacement count=$count\n" unless $count == 1;
' "$tmpdir/INSPIRATION.md"

git show "$base_commit:README.md" > "$tmpdir/README.md"
printf '%s\n' \
	'' \
	'## SIMD Predicate Kernels' \
	'' \
	'Allocation-free typed int64 predicate masks use an AVX2 kernel for equality and inequality when supported, with portable fallback elsewhere. See [C015 SIMD predicate kernels](C015_SIMD.md) for the API, validation, and measurements.' \
	>> "$tmpdir/README.md"

mkdir -p "$tmpdir/hat/hatPredicate"
git show "$base_commit:hat/hatPredicate/mask.go" > "$tmpdir/hat/hatPredicate/mask.go"
bash "$repo_root/scripts/apply-c015-fastpath.sh" "$tmpdir"

git add -- \
	C015_SIMD.md \
	hat/hatPredicate/simd.go \
	hat/hatPredicate/simd_portable.go \
	hat/hatPredicate/simd_amd64.go \
	hat/hatPredicate/simd_avx2_amd64.s \
	hat/hatPredicate/simd_test.go \
	hat/hatPredicate/simd_benchmark_test.go \
	scripts/apply-c015-fastpath.sh \
	scripts/benchmark-c015.sh \
	scripts/deliver-c015.sh \
	scripts/format-c015.sh \
	scripts/race-c015.sh \
	scripts/test-c015.sh

make_blob() {
	local source="$1"
	local destination="$2"
	local blob
	blob="$(git hash-object -w "$source")"
	git update-index --add --cacheinfo "100644,$blob,$destination"
}

make_blob "$tmpdir/Makefile" Makefile
make_blob "$tmpdir/INSPIRATION.md" INSPIRATION.md
make_blob "$tmpdir/README.md" README.md
make_blob "$tmpdir/hat/hatPredicate/mask.go" hat/hatPredicate/mask.go

git diff --cached --check
git diff --cached --stat
git diff --cached --name-status

if [[ "$mode" == preview ]]; then
	exit 0
fi

if [[ "$(git rev-parse HEAD)" != "$base_commit" ]]; then
	echo "HEAD changed while preparing C015 delivery" >&2
	exit 1
fi

git commit -m 'feat(predicate): add AVX2 mask kernels'

if [[ "$mode" == push ]]; then
	git push origin "$branch"
fi
