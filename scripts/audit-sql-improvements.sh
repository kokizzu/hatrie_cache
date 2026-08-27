#!/bin/sh
set -eu

if [ "${1:-}" = "matrix" ]; then
	printf '%s\n' 'Relational test matrix:'
	sed -n '21,90p' ./SQL_TEST_MATRIX.md
	printf '%s\n' '' 'Feature test declarations:'
	rg -n '^func Test' \
	  ./hat/hatSql/correlated_subquery_test.go \
	  ./hat/hatSql/lateral_test.go \
	  ./hat/hatSql/aggregate_filter_test.go \
	  ./hat/hatSql/grouping_sets_test.go \
	  ./hat/hatSql/named_window_test.go \
	  ./hat/hatSql/time_zone_test.go \
	  ./hat/hatSql/regex_test.go \
	  ./hat/hatSql/parameterized_view_test.go \
	  ./hat/hatSql/rewrite_test.go \
	  ./hat/hatSql/pivot_test.go
	exit 0
fi

printf '%s\n' 'SQL documentation files:'
rg --files | rg '(^|/)(README|SQL|QUERY).*\.md$' || true

printf '%s\n' '' 'Feature test coverage:'
rg -l 'EXISTS|LATERAL|FILTER|ROLLUP|CUBE|GROUPING SETS|PivotRows|UnpivotRows|WINDOW|LEAD|LAG|REGEXP|TIMESTAMP|ParameterizedView|rewrite' ./hat/hatSql/*_test.go | sort

printf '%s\n' '' 'Feature implementation files:'
rg -l 'parseSQLGroupingClause|PivotRows|evalSQLExists|executeSQLLateralSource|FILTER|resolveSQLNamedWindows|PARSE_TIMESTAMP|REGEXP|ParameterizedViews|rewriteSQLQuery' ./hat/hatSql/*.go | sort

printf '%s\n' '' 'SQL documentation headings:'
rg -n '^#{1,3} ' ./SQL.md ./SQL_COMPATIBILITY.md ./SQL_TEST_MATRIX.md

printf '%s\n' '' 'Existing feature documentation references:'
rg -n 'EXISTS|LATERAL|FILTER|ROLLUP|CUBE|GROUPING SETS|PIVOT|WINDOW|LEAD|LAG|REGEXP|TIME ZONE|Parameterized|rewrite' ./SQL.md ./SQL_COMPATIBILITY.md ./SQL_TEST_MATRIX.md || true

printf '%s\n' '' 'Relevant SQL documentation sections:'
sed -n '338,460p' ./SQL.md
sed -n '832,951p' ./SQL.md
sed -n '1113,1240p' ./SQL.md
sed -n '1542,1585p' ./SQL.md

printf '%s\n' '' 'Grammar checklist entries to update:'
rg -n -C 2 'LEFT \[OUTER\]|WHERE.*LIKE|GROUP BY.*HAVING|ROW_NUMBER.*RANK|Uncorrelated derived|Correlated subqueries remain' ./SQL.md || true

printf '%s\n' '' 'Executable feature examples:'
sed -n '1,160p' ./hat/hatSql/correlated_subquery_test.go
sed -n '1,140p' ./hat/hatSql/lateral_test.go
sed -n '1,160p' ./hat/hatSql/aggregate_filter_test.go
sed -n '1,160p' ./hat/hatSql/named_window_test.go
sed -n '1,160p' ./hat/hatSql/time_zone_test.go
sed -n '1,160p' ./hat/hatSql/regex_test.go
