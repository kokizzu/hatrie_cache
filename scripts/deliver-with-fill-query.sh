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
tmpdir="$(mktemp -d "$repo_root/.with-fill-delivery.XXXXXX")"
trap 'rm -rf -- "$tmpdir"' EXIT
index="$tmpdir/index"
export GIT_INDEX_FILE="$index"
git read-tree "$base_commit"

git show "$base_commit:Makefile" > "$tmpdir/Makefile"
if rg -q '^# with-fill-query-targets$' "$tmpdir/Makefile"; then
	echo "WITH FILL targets already exist in HEAD" >&2
	exit 1
fi
printf '%s\n' \
	'' \
	'# with-fill-query-targets' \
	'format-with-fill-query:' \
	$'\tbash ./scripts/format-with-fill-query.sh' \
	'' \
	'test-with-fill-query:' \
	$'\tbash ./scripts/test-with-fill-query.sh' \
	'' \
	'test-with-fill-package:' \
	$'\tbash ./scripts/test-with-fill-package.sh' \
	'' \
	'race-with-fill-query:' \
	$'\tbash ./scripts/race-with-fill-query.sh' \
	'' \
	'benchmark-with-fill-query:' \
	$'\tbash ./scripts/benchmark-with-fill-query.sh' \
	'' \
	'deliver-with-fill-query:' \
	$'\tbash ./scripts/deliver-with-fill-query.sh preview' \
	'' \
	'commit-with-fill-query:' \
	$'\tbash ./scripts/deliver-with-fill-query.sh commit' \
	'' \
	'push-with-fill-query:' \
	$'\tbash ./scripts/deliver-with-fill-query.sh push' \
	>> "$tmpdir/Makefile"

git show "$base_commit:INSPIRATION.md" > "$tmpdir/INSPIRATION.md"
perl -0pi -e 'my $count = s/(- \[x\] C081a Ordered time-series gap filling with explicit half-open bounds\.)/$1\n- [x] C081b SQL WITH FILL grammar for bounded TIMESTAMP\/DURATION ordered series./g; die "C081b checklist insertion count=$count\n" unless $count == 1' "$tmpdir/INSPIRATION.md"

git show "$base_commit:README.md" > "$tmpdir/README.md"
perl -0pi -e 'my $count = s/(- Per-group SQL top-N selection: \[SQL `LIMIT BY`\]\(SQL_LIMIT_BY\.md\))/$1\n- Bounded ordered time-series gap filling: [SQL `WITH FILL`](WITH_FILL.md)/g; die "README WITH FILL link insertion count=$count\n" unless $count == 1' "$tmpdir/README.md"

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

sub replace_once_after {
    my ($text, $anchor, $old, $new, $label) = @_;
    my $start = index($text, $anchor);
    die "$label anchor not found\n" if $start < 0;
    my $first = index($text, $old, $start);
    my $second = $first < 0 ? -1 : index($text, $old, $first + length($old));
    die "$label replacement count is not exactly one\n" if $first < 0 || $second >= 0;
    substr($text, $first, length($old), $new);
    return $text;
}

my $order_field = <<'EOF';
	collation  SQLCollation
EOF
$source = replace_once($source, $order_field, $order_field . "\tfill       *sqlOrderFill\n", 'ORDER BY fill state');

my $order_parser = <<'EOF';
		if p.keyword("NULLS") {
			p.next()
			if p.keyword("FIRST") {
				value.nullsFirst = true
				p.next()
			} else if p.keyword("LAST") {
				value.nullsLast = true
				p.next()
			} else {
				return nil, p.expected(p.current(), "FIRST or LAST after NULLS", []string{"FIRST", "LAST"})
			}
		}
EOF
my $order_fill = <<'EOF';
		if p.keyword("WITH") {
			if len(out) > 0 || expr.kind != "field" || expr.qualifier != "" {
				return nil, p.diagnostic(p.current(), "WITH FILL requires the only ORDER BY item to be an unqualified selected field")
			}
			if value.desc {
				return nil, p.diagnostic(p.current(), "WITH FILL requires ascending ORDER BY")
			}
			p.next()
			if err := p.expectKeyword("FILL"); err != nil {
				return nil, err
			}
			spec, err := p.parseSQLWithFillSpec(expr.name)
			if err != nil {
				return nil, err
			}
			value.fill = &sqlOrderFill{spec: spec}
		}
EOF
$source = replace_once($source, $order_parser, $order_parser . $order_fill, 'WITH FILL parser');

my $clone_line = "\t\tcopy[index].expr = cloneSQLExpr(order.expr)\n";
my $clone_addition = <<'EOF';
		if order.fill != nil {
			fill := *order.fill
			fill.spec.Template = cloneSQLFillRow(order.fill.spec.Template)
			copy[index].fill = &fill
		}
EOF
$source = replace_once($source, $clone_line, $clone_line . $clone_addition, 'WITH FILL query clone');

$source = replace_once($source,
    'if metrics == nil && query.limitBy == nil && sqlIndexedMaterializedOrderStreamable(query, resolver, options) {',
    'if metrics == nil && !sqlQueryHasWithFill(query) && query.limitBy == nil && sqlIndexedMaterializedOrderStreamable(query, resolver, options) {',
    'materialized indexed fill guard');
$source = replace_once($source,
    'if metrics == nil && query.limitBy == nil && sqlTopNMaterializedStreamable(query, resolver) {',
    'if metrics == nil && !sqlQueryHasWithFill(query) && query.limitBy == nil && sqlTopNMaterializedStreamable(query, resolver) {',
    'materialized top-n fill guard');

my $rows_parser_fallback = <<'EOF';
	if sqlQueryHasWithFill(query) {
		result, err := executeSQLQueryWithMetrics(query, resolver, nil, nil, control)
		if err != nil {
			return err
		}
		columns := sqlColumns(query.selects)
		for _, row := range result.Rows {
			if err := visit(columns, row); err != nil {
				return err
			}
		}
		return nil
	}
EOF
$source = replace_once($source, 'func executeSQLQueryRowsParsed', $rows_parser_fallback . 'func executeSQLQueryRowsParsed', 'row-stream WITH FILL fallback');

my $outer = 'func executeSQLQueryWithMetricsOuter(q *sqlQuery, resolver SQLSourceResolver, ctes map[string][]SQLRow, metrics *sqlExecutionMetrics, control *sqlExecutionControl, outer *sqlExecRow) (SQLQueryResult, error) {';
my $outer_initial_old = <<'EOF';
	if q.limitBy == nil {
		if result, handled, runtimeErr := executeSQLRuntimeJoinFilter(q, resolver, control, metrics); handled {
			return result, runtimeErr
		}
		if result, handled, spillErr := executeSQLSpillHashJoin(q, resolver, control, metrics); handled {
			return result, spillErr
		}
		if result, handled, streamErr := executeSQLStreamedSpilledGroupAggregate(q, resolver, control, metrics, nil); handled {
			return result, streamErr
		}
	}
	if result, handled, streamErr := executeSQLHashGroupAggregateStream(q, resolver, control, metrics, nil); handled {
		return result, streamErr
	}
EOF
my $outer_initial_new = <<'EOF';
	if !sqlQueryHasWithFill(q) && q.limitBy == nil {
		if result, handled, runtimeErr := executeSQLRuntimeJoinFilter(q, resolver, control, metrics); handled {
			return result, runtimeErr
		}
		if result, handled, spillErr := executeSQLSpillHashJoin(q, resolver, control, metrics); handled {
			return result, spillErr
		}
		if result, handled, streamErr := executeSQLStreamedSpilledGroupAggregate(q, resolver, control, metrics, nil); handled {
			return result, streamErr
		}
	}
	if !sqlQueryHasWithFill(q) {
		if result, handled, streamErr := executeSQLHashGroupAggregateStream(q, resolver, control, metrics, nil); handled {
			return result, streamErr
		}
	}
EOF
$source = replace_once_after($source, $outer, $outer_initial_old, $outer_initial_new, 'materialized fast-path fill guards');

my $columnar_fill_old = <<'EOF';
		if !indexed && q.sample == nil {
			if result, handled, err := executeSQLColumnarScan(q, resolver, control, metrics, outer); handled {
				return result, err
			}
		}
EOF
my $columnar_fill_new = <<'EOF';
		if !indexed && q.sample == nil && !sqlQueryHasWithFill(q) {
			if result, handled, err := executeSQLColumnarScan(q, resolver, control, metrics, outer); handled {
				return result, err
			}
		}
EOF
$source = replace_once_after($source, $outer, $columnar_fill_old, $columnar_fill_new, 'columnar fill guard');

$source = replace_once_after($source, $outer,
    'if indexOrdered {',
    'if indexOrdered && !sqlQueryHasWithFill(q) {',
    'ordered aggregate fill guard');

my $hash_rows_old = <<'EOF';
	if result, handled, err := executeSQLHashGroupAggregateRows(q, func(consume func(sqlExecRow) error) error {
		for _, row := range rows {
			if err := consume(row); err != nil {
				return err
			}
		}
		return nil
	}, control, metrics, nil); handled {
		return result, err
	}
EOF
my $hash_rows_new = <<'EOF';
	if !sqlQueryHasWithFill(q) {
		if result, handled, err := executeSQLHashGroupAggregateRows(q, func(consume func(sqlExecRow) error) error {
			for _, row := range rows {
				if err := consume(row); err != nil {
					return err
				}
			}
			return nil
		}, control, metrics, nil); handled {
			return result, err
		}
	}
EOF
$source = replace_once_after($source, $outer, $hash_rows_old, $hash_rows_new, 'hash aggregate fill guard');

$source = replace_once_after($source, $outer,
    'if control != nil && control.options.MaxSortBytes > 0 {',
    'if control != nil && control.options.MaxSortBytes > 0 && !sqlQueryHasWithFill(q) {',
    'external sort fill guard');

my $fill_tail = <<'EOF';
	if !externallySorted {
		if q.limitBy != nil && !limitByApplied {
EOF
my $fill_block = <<'EOF';
	if !externallySorted && sqlQueryHasWithFill(q) {
		started = time.Now()
		inputRows := len(out)
		rows := make([]SQLRow, len(out))
		for index := range out {
			rows[index] = out[index].row
		}
		filled, err := applySQLWithFill(q, result.Columns, rows, control.maxRows)
		if err != nil {
			return SQLQueryResult{}, err
		}
		out = make([]sqlQueryOutput, len(filled))
		for index := range filled {
			out[index].row = filled[index]
		}
		metrics.record("WITH FILL", sqlExplainOrders(q.orderBy), inputRows, len(out), started)
	}
EOF
$source = replace_once_after($source, $outer, $fill_tail, $fill_block . $fill_tail, 'WITH FILL result expansion');

my $explain_tail = <<'EOF';
		} else if order.nullsLast {
			values[index] += " NULLS LAST"
		}
EOF
$source = replace_once($source, $explain_tail, $explain_tail . "\t\tif order.fill != nil {\n\t\t\tvalues[index] += sqlExplainWithFill(order.fill.spec)\n\t\t}\n", 'WITH FILL explain output');

my $columnar_guard = <<'EOF';
	if sqlQueryHasWithFill(q) {
		return SQLQueryResult{}, false, nil
	}
EOF
$source = replace_once($source, 'func executeSQLColumnarScan', $columnar_guard . 'func executeSQLColumnarScan', 'columnar WITH FILL fallback');

open my $output, '>', $path or die "open $path for write: $!\n";
print {$output} $source or die "write $path: $!\n";
close $output or die "close $path: $!\n";
PERL

git show "$base_commit:hat/hatSql/hash_group_aggregate.go" > "$tmpdir/hash_group_aggregate.go"
perl - "$tmpdir/hash_group_aggregate.go" <<'PERL'
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

$source = replace_once($source,
    'if !sqlHashGroupAggregatePlan(query) || control == nil',
    'if sqlQueryHasWithFill(query) || !sqlHashGroupAggregatePlan(query) || control == nil',
    'hash aggregate streamability fill guard');

my $guard = <<'EOF';
	if sqlQueryHasWithFill(q) {
		return SQLQueryResult{}, false, nil
	}
EOF
$source = replace_once($source, 'func executeSQLHashGroupAggregateStream', $guard . 'func executeSQLHashGroupAggregateStream', 'hash aggregate stream fill fallback');

open my $output, '>', $path or die "open $path for write: $!\n";
print {$output} $source or die "write $path: $!\n";
close $output or die "close $path: $!\n";
PERL

git add -- \
	WITH_FILL.md \
	README.md \
	INSPIRATION.md \
	hat/hatSql/hash_group_aggregate.go \
	hat/hatSql/query.go \
	hat/hatSql/with_fill.go \
	hat/hatSql/with_fill_query.go \
	hat/hatSql/with_fill_query_test.go \
	scripts/benchmark-with-fill-query.sh \
	scripts/deliver-with-fill-query.sh \
	scripts/format-with-fill-query.sh \
	scripts/race-with-fill-query.sh \
	scripts/test-with-fill-package.sh \
	scripts/test-with-fill-query.sh

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
make_blob "$tmpdir/query.go" hat/hatSql/query.go

git diff --cached --check
git diff --cached --stat
git diff --cached --name-status

if [[ "$mode" == preview ]]; then
	exit 0
fi

if [[ "$(git rev-parse HEAD)" != "$base_commit" ]]; then
	echo "HEAD changed while preparing WITH FILL delivery" >&2
	exit 1
fi

git commit -m 'feat(sql): add WITH FILL gap filling'

if [[ "$mode" == push ]]; then
	git push origin "$branch"
fi
