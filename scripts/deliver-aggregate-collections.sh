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
tmpdir="$(mktemp -d "$repo_root/.aggregate-collections-delivery.XXXXXX")"
trap 'rm -rf -- "$tmpdir"' EXIT
index="$tmpdir/index"
export GIT_INDEX_FILE="$index"
git read-tree "$base_commit"

git show "$base_commit:Makefile" > "$tmpdir/Makefile"
if rg -q '^# aggregate-collections-targets$' "$tmpdir/Makefile"; then
	echo "aggregate collection targets already exist in HEAD" >&2
	exit 1
fi
printf '%s\n' \
    '' \
    '# aggregate-collections-targets' \
    'format-aggregate-collections:' \
    $'\tbash ./scripts/format-aggregate-collections.sh' \
    '' \
    'test-aggregate-collections:' \
    $'\tbash ./scripts/test-aggregate-collections.sh' \
    '' \
    'test-race-aggregate-collections:' \
    $'\tbash ./scripts/test-race-aggregate-collections.sh' \
    '' \
    'benchmark-aggregate-collections:' \
    $'\tbash ./scripts/benchmark-aggregate-collections.sh' \
    '' \
    'deliver-aggregate-collections:' \
    $'\tbash ./scripts/deliver-aggregate-collections.sh preview' \
    '' \
    'commit-aggregate-collections:' \
    $'\tbash ./scripts/deliver-aggregate-collections.sh commit' \
    '' \
    'push-aggregate-collections:' \
    $'\tbash ./scripts/deliver-aggregate-collections.sh push' \
    >> "$tmpdir/Makefile"

git show "$base_commit:INSPIRATION.md" > "$tmpdir/INSPIRATION.md"
perl -0pi -e 'my $count = s/- \[ \] C084 Array and map aggregate functions\./- [x] C084a Array and map aggregate functions with deterministic NULL and duplicate-key semantics./g; die "C084 checklist replacement count=$count\n" unless $count == 1' "$tmpdir/INSPIRATION.md"

git show "$base_commit:hat/hatSql/query.go" > "$tmpdir/query.go"
perl - "$tmpdir/query.go" <<'PERL'
use strict;
use warnings;

my ($path) = @ARGV;
open my $input, '<', $path or die "open $path: $!\n";
local $/;
my $source = <$input>;
close $input or die "close $path: $!\n";

sub replace_once {
    my ($text, $old, $new, $label) = @_;
    my $first = index($text, $old);
    my $second = $first < 0 ? -1 : index($text, $old, $first + length($old));
    die "$label replacement count is not exactly one\n" if $first < 0 || $second >= 0;
    substr($text, $first, length($old), $new);
    return $text;
}

sub replace_case_after {
    my ($text, $switch, $old, $new, $label) = @_;
    my $pattern = qr{(\Q$switch\E\n[ \t]*)\Q$old\E};
    my $count = ($text =~ s{$pattern}{$1$new}g);
    die "$label replacement count is not exactly one\n" unless $count == 1;
    return $text;
}

$source = replace_case_after(
    $source,
    'switch expr.name {',
    qq{case "COUNT", "SUM", "AVG", "MIN", "MAX", "APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE", "APPROX_TOP_K":},
    qq{case "COUNT", "SUM", "AVG", "MIN", "MAX", "APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE", "APPROX_TOP_K", "ARRAY_AGG", "GROUP_ARRAY", "GROUP_UNIQ_ARRAY", "MAP_AGG":},
    'aggregate recognition',
);

$source = replace_case_after(
    $source,
    'switch upper {',
    qq{case "COUNT", "SUM", "AVG", "MIN", "MAX", "APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE", "APPROX_TOP_K":},
    qq{case "COUNT", "SUM", "AVG", "MIN", "MAX", "APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE", "APPROX_TOP_K", "ARRAY_AGG", "GROUP_ARRAY", "GROUP_UNIQ_ARRAY", "MAP_AGG":},
    'FILTER validation',
);

$source = replace_case_after(
    $source,
    'switch strings.ToUpper(name) {',
    qq{case "COALESCE", "LOWER", "NULLIF", "CONTAINS", "ARRAY_CONTAINS", "COUNT", "SUM", "AVG", "MIN", "MAX", "APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE", "APPROX_TOP_K", "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS", "REGEXP_LIKE", "REGEXP_EXTRACT", "PARSE_TIMESTAMP", "TIMESTAMP_ADD", "TIMESTAMP_DIFF":},
    qq{case "COALESCE", "LOWER", "NULLIF", "CONTAINS", "ARRAY_CONTAINS", "COUNT", "SUM", "AVG", "MIN", "MAX", "APPROX_COUNT_DISTINCT", "APPROX_PERCENTILE", "APPROX_TOP_K", "ARRAY_AGG", "GROUP_ARRAY", "GROUP_UNIQ_ARRAY", "MAP_AGG", "JSON_VALUE", "JSON_QUERY", "JSON_EXISTS", "REGEXP_LIKE", "REGEXP_EXTRACT", "PARSE_TIMESTAMP", "TIMESTAMP_ADD", "TIMESTAMP_DIFF":},
    'built-in function validation',
);

my $aggregate_marker = qq{\t\tcase "COUNT":\n\t\t\taggregateRows, err := sqlAggregateFilterRows(expr, group)};
my $aggregate_code = <<'EOF';
		case "ARRAY_AGG", "GROUP_ARRAY":
			values, err := sqlAggregateCollectionValues(expr, group)
			if err != nil {
				return sqlEvaluationFailure(err)
			}
			return values
		case "GROUP_UNIQ_ARRAY":
			values, err := sqlAggregateCollectionValues(expr, group)
			if err != nil {
				return sqlEvaluationFailure(err)
			}
			return sqlAggregateUniqueCollection(values)
		case "MAP_AGG":
			values, err := sqlAggregateMapValues(expr, group)
			if err != nil {
				return sqlEvaluationFailure(err)
			}
			return values
EOF
$source = replace_once($source, $aggregate_marker, $aggregate_code . $aggregate_marker, 'collection evaluator');

open my $output, '>', $path or die "open $path for write: $!\n";
print {$output} $source or die "write $path: $!\n";
close $output or die "close $path: $!\n";
PERL

git add -- \
    AGGREGATE_COLLECTIONS.md \
    hat/hatSql/aggregate_collections.go \
    hat/hatSql/aggregate_collection_test.go \
    scripts/benchmark-aggregate-collections.sh \
    scripts/deliver-aggregate-collections.sh \
    scripts/format-aggregate-collections.sh \
    scripts/test-aggregate-collections.sh \
    scripts/test-race-aggregate-collections.sh

make_blob() {
	local source="$1"
	local destination="$2"
	local blob
	blob="$(git hash-object -w "$source")"
	git update-index --add --cacheinfo "100644,$blob,$destination"
}

make_blob "$tmpdir/Makefile" Makefile
make_blob "$tmpdir/INSPIRATION.md" INSPIRATION.md
make_blob "$tmpdir/query.go" hat/hatSql/query.go

git diff --cached --check
git diff --cached --stat
git diff --cached --name-status

if [[ "$mode" == preview ]]; then
	exit 0
fi

if [[ "$(git rev-parse HEAD)" != "$base_commit" ]]; then
	echo "HEAD changed while preparing aggregate collection delivery" >&2
	exit 1
fi

git commit -m 'feat(sql): add array and map aggregates'

if [[ "$mode" == push ]]; then
	git push origin "$branch"
fi
