#!/bin/sh
set -eu

go test ./hat/hatSql -run '^(TestSQLTableSampleBernoulliRepeatable|TestSQLTableSampleReservoirRepeatable|TestSQLTableSampleStreamsMaterializedSemantics|TestSQLTableSampleRetainsSourceRowBudget|TestSQLTableSampleValidation)$'
