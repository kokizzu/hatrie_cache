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
tmpdir="$(mktemp -d "$repo_root/.array-join-delivery.XXXXXX")"
trap 'rm -rf -- "$tmpdir"' EXIT
index="$tmpdir/index"
export GIT_INDEX_FILE="$index"
git read-tree "$base_commit"

git show "$base_commit:Makefile" > "$tmpdir/Makefile"
if rg -q '^# array-join-targets$' "$tmpdir/Makefile"; then
	echo "array join targets already exist in HEAD" >&2
	exit 1
fi
printf '%s\n' \
    '' \
    '# array-join-targets' \
    'format-array-join:' \
    $'\tbash ./scripts/format-array-join.sh' \
    '' \
    'test-array-join:' \
    $'\tbash ./scripts/test-array-join.sh' \
    '' \
    'test-race-array-join:' \
    $'\tbash ./scripts/test-race-array-join.sh' \
    '' \
    'benchmark-array-join:' \
    $'\tbash ./scripts/benchmark-array-join.sh' \
    '' \
    'deliver-array-join:' \
    $'\tbash ./scripts/deliver-array-join.sh preview' \
    '' \
    'commit-array-join:' \
    $'\tbash ./scripts/deliver-array-join.sh commit' \
    '' \
    'push-array-join:' \
    $'\tbash ./scripts/deliver-array-join.sh push' \
    >> "$tmpdir/Makefile"

git show "$base_commit:INSPIRATION.md" > "$tmpdir/INSPIRATION.md"
perl -0pi -e 'my $count = s/- \[ \] C083 ArrayJoin-style row expansion\./- [x] C083a ArrayJoin-style row expansion for array and slice values./g; die "C083 checklist replacement count=$count\n" unless $count == 1' "$tmpdir/INSPIRATION.md"

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

my $array_case = <<'EOF';
		case p.keyword("ARRAY"):
			if q.from == nil {
				return nil, p.diagnostic(p.current(), "ARRAY JOIN requires FROM first")
			}
			join, err := p.parseArrayJoin()
			if err != nil {
				return nil, err
			}
			q.joins = append(q.joins, join)
EOF
$source = replace_once(
    $source,
    qq{\t\t\tq.joins = append(q.joins, join)\n\t\tcase p.keyword("TABLESAMPLE"):},
    qq{\t\t\tq.joins = append(q.joins, join)\n} . $array_case . qq{\t\tcase p.keyword("TABLESAMPLE"):},
    'ARRAY JOIN parser clause',
);

my $array_parser = <<'EOF';
func (p *sqlQueryParser) parseArrayJoin() (sqlJoin, error) {
	token := p.current()
	p.next()
	if err := p.expectKeyword("JOIN"); err != nil {
		return sqlJoin{}, err
	}
	expression, err := p.parsePrimary()
	if err != nil {
		return sqlJoin{}, err
	}
	if expression.kind == "" {
		return sqlJoin{}, p.diagnostic(token, "ARRAY JOIN requires an array expression")
	}
	alias := ""
	if p.keyword("AS") {
		p.next()
		name, err := p.expectIdentifier("an ARRAY JOIN alias", nil)
		if err != nil {
			return sqlJoin{}, err
		}
		alias = name.text
	} else if p.current().kind == sqlTokenIdentifier && !sqlClauseKeyword(p.current().text) {
		alias = p.current().text
		p.next()
	}
	if alias == "" {
		if expression.kind == "field" {
			alias = expression.name
		} else {
			alias = "array"
		}
	}
	return sqlJoin{kind: "ARRAY", source: sqlSource{kind: "ARRAY", alias: alias}, on: expression}, nil
}

EOF
$source = replace_once(
    $source,
    qq!func (p *sqlQueryParser) parseAlias(source *sqlSource) error {\n!,
    $array_parser . qq!func (p *sqlQueryParser) parseAlias(source *sqlSource) error {\n!,
    'ARRAY JOIN parser helper',
);

my $array_execution = <<'EOF';
			if join.kind == "ARRAY" {
				inputRows := len(rows)
				next := make([]sqlExecRow, 0, len(rows))
				for _, left := range rows {
					value := evalSQLExpr(join.on, []sqlExecRow{left}, left)
					if err := sqlExpressionError(value); err != nil {
						return SQLQueryResult{}, err
					}
					if value == nil {
						continue
					}
					elements, ok := sqlArrayJoinElements(value)
					if !ok {
						return SQLQueryResult{}, fmt.Errorf("ARRAY JOIN expression must evaluate to an array, got %T", value)
					}
					for index := 0; index < elements.Len(); index++ {
						element := elements.Index(index).Interface()
						if err := control.addJoinWork(1); err != nil {
							return SQLQueryResult{}, err
						}
						candidate := SQLRow{join.source.alias: element}
						combined := mergeSQLRows(sqlExecRow{sources: map[string]SQLRow{join.source.alias: candidate}, order: []string{join.source.alias}}, left)
						next = append(next, combined)
						if len(next) > maxRows {
							return SQLQueryResult{}, fmt.Errorf("SQL ARRAY JOIN exceeds the %d row limit", maxRows)
						}
					}
				}
				metrics.record("ARRAY JOIN", sqlExplainExpression(join.on)+" AS "+join.source.alias, inputRows, len(next), started)
				rows = next
				leftAliases = append(leftAliases, join.source.alias)
				continue
			}
EOF
$source = replace_once(
    $source,
    qq!\t\t\tif join.source.lateral {\n!,
    $array_execution . qq!\t\t\tif join.source.lateral {\n!,
    'ARRAY JOIN execution',
);

my $array_explain = <<'EOF';
		if join.kind == "ARRAY" {
			detail := sqlExplainExpression(join.on)
			if join.source.alias != "" {
				detail += " AS " + join.source.alias
			}
			*steps = append(*steps, SQLExplainStep{Node: prefix + "ARRAY JOIN", Detail: detail})
			if join.source.alias != "" {
				leftAliases = append(leftAliases, join.source.alias)
			}
			continue
		}
EOF
$source = replace_once(
    $source,
    qq!\tfor _, join := range query.joins {\n\t\tdetail := join.kind + " JOIN " + sqlExplainSource(join.source)\n!,
    qq!\tfor _, join := range query.joins {\n! . $array_explain . qq!\t\tdetail := join.kind + " JOIN " + sqlExplainSource(join.source)\n!,
    'ARRAY JOIN explain plan',
);

$source = replace_once(
    $source,
    qq{"TABLESAMPLE", "WHERE"},
    qq{"TABLESAMPLE", "ARRAY", "WHERE"},
    'ARRAY JOIN clause keyword',
);

open my $output, '>', $path or die "open $path for write: $!\n";
print {$output} $source or die "write $path: $!\n";
close $output or die "close $path: $!\n";
PERL

git add -- \
    ARRAY_JOIN.md \
    hat/hatSql/array_join.go \
    hat/hatSql/array_join_test.go \
    scripts/benchmark-array-join.sh \
    scripts/deliver-array-join.sh \
    scripts/format-array-join.sh \
    scripts/test-array-join.sh \
    scripts/test-race-array-join.sh

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
	echo "HEAD changed while preparing ARRAY JOIN delivery" >&2
	exit 1
fi

git commit -m 'feat(sql): add array join expansion'

if [[ "$mode" == push ]]; then
	git push origin "$branch"
fi
