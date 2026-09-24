#!/usr/bin/env bash
set -euo pipefail

gofmt -w hat/hatSql/m_u36_arrangement_hydration_admission.go hat/hatSql/m_u36_arrangement_hydration_admission_test.go hat/hatSql/m_u36_arrangement_hydration_admission_benchmark_test.go hat/hatSql/typed_table_arrangements.go hat/hatSql/typed_table_join_arrangements.go
