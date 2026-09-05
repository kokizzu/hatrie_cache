#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestSQLRewrite(EliminatesDuplicateDeterministicBooleanSubexpression|KeepsNonDuplicateBooleanExpressions|DuplicatePredicatesPreserveThreeValuedFiltering|DoesNotEliminateUserFunctionSubexpressions|EliminatesDuplicateCastPredicate)$' -count=1
