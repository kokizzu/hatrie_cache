#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql \
  -run 'TestSQLRollupCubeAndGroupingSets|TestSQLGroupingSetsGroupingIdentifiers|TestSQLRollupAndCubeGroupingIdentifiers|TestSQLGroupingIdentifierNestedExpressionAndValidation|TestSQLGroupingIdentifierOrdinaryGroupDefaultsToZero' \
  -count=1
