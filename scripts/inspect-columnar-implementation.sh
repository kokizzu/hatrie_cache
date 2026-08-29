#!/usr/bin/env sh
set -eu

printf '%s\n' '=== columnar type locations ==='
grep -R -n -E 'type (ColumnarBatch|ColumnarSourceResolver)|ResolveSQLColumnarSource|executeSQLColumnar|sqlCanUseColumnar' hat --include='*.go' || true

printf '%s\n' '=== columnar model and executor excerpts ==='
sed -n '1,260p' hat/hatSql/model.go
sed -n '1,100p' hat/hatSql/contracts.go
sed -n '6600,6880p' hat/hatSql/query.go
grep -n -E 'func (evalSQLAggregate|sqlQueryHasAggregate|evalSQLExprBatch)|type sqlExpr struct' hat/hatSql/query.go || true
sed -n '10900,11450p' hat/hatSql/query.go
sed -n '1,260p' hat/hatCache/sql_query.go
sed -n '1790,1900p' hat/hatCache/sql_query.go
