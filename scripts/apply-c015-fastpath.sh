#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 ]]; then
	echo "usage: $0 checkout-root" >&2
	exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
checkout_root="$1"
predicate_dir="$checkout_root/hat/hatPredicate"
mask_file="$predicate_dir/mask.go"

cp "$repo_root/hat/hatPredicate/simd.go" "$predicate_dir/simd.go"
cp "$repo_root/hat/hatPredicate/simd_portable.go" "$predicate_dir/simd_portable.go"
cp "$repo_root/hat/hatPredicate/simd_amd64.go" "$predicate_dir/simd_amd64.go"
cp "$repo_root/hat/hatPredicate/simd_avx2_amd64.s" "$predicate_dir/simd_avx2_amd64.s"

perl -0pi -e '
my $header = "func MatchInt64(mask []uint64, values []int64, predicate Int64Predicate, target int64) (int, error) {\n";
my $start = index($_, $header);
my $second = $start < 0 ? -1 : index($_, $header, $start + length($header));
die "MatchInt64 header count is not exactly one\n" if $start < 0 || $second >= 0;
my $end = index($_, "\n}\n\n// MatchString", $start);
die "MatchInt64 body end not found\n" if $end < 0;
$end += 2;
my $replacement = "func MatchInt64(mask []uint64, values []int64, predicate Int64Predicate, target int64) (int, error) {\n\treturn MatchInt64SIMD(mask, values, predicate, target)\n}";
substr($_, $start, $end - $start, $replacement);
' "$mask_file"
