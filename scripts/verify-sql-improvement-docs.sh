#!/bin/sh
set -eu

for marker in \
  'EXISTS (query)' \
  'LATERAL (query)' \
  'FILTER (WHERE ...)' \
  'GROUPING SETS' \
  'PivotRows' \
  'WINDOW name AS' \
  'PARSE_TIMESTAMP' \
  'REGEXP_EXTRACT' \
  'ParameterizedViews' \
  'execution-local rewrite pass'
do
	if ! rg -F -q "$marker" ./SQL.md; then
		printf 'missing SQL documentation marker: %s\n' "$marker" >&2
		exit 1
	fi
done

if rg -F -q 'Correlated subqueries remain explicitly out of scope' ./SQL.md; then
	printf '%s\n' 'obsolete correlated-subquery limitation remains in SQL.md' >&2
	exit 1
fi

for test_name in \
  'TestSQLCorrelatedExistsNotExistsAndScalarSubqueries' \
  'TestSQLLateralJoinEvaluatesDerivedSourceForEachOuterRow' \
  'TestSQLAggregateFilterClause' \
  'TestSQLRollupCubeAndGroupingSets' \
  'TestPivotAndUnpivotRows' \
  'TestSQLNamedWindowsResolveForRankingAndLeadLag' \
  'TestSQLTimeZoneParsingAndArithmetic' \
  'TestSQLRegexPredicatesAndExtraction' \
  'TestParameterizedViewsCacheByArgumentsAndInvalidateDependencies' \
  'TestSQLRewriteFoldsConstantsAndEliminatesDeadDerivedProjection'
do
	if ! rg -F -q "$test_name" ./SQL_TEST_MATRIX.md; then
		printf 'missing matrix evidence: %s\n' "$test_name" >&2
		exit 1
	fi
done
