#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/sql_mutation_admission.go hat/hatSql/ch013_mutation_admission_test.go hat/hatSql/query.go hat/hatCache/sql.go hat/hatCache/ch013_mutation_admission_test.go hat/hatCache/ch013_mutation_admission_benchmark_test.go
