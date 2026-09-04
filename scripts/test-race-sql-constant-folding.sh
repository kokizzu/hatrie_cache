#!/bin/sh
set -eu

go test -race ./hat/hatSql -run '^TestSQLRewrite(FoldsConstantScalarExpressions|DoesNotFoldRowDependentOrAggregateExpressions|DoesNotFoldUnknownFunctions|PreservesConstantNullPredicateSemantics)$' -count=1
