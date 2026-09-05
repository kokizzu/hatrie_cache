#!/bin/sh
set -eu

go test ./hat/hatSql -run 'TestSQLColumnar(NumericPredicateOrder|OrdersNumericPredicates)' -count=1
