#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatCache -run '^(TestT232|TestSQLTransaction|TestRunAtomic|TestCompileSQLAtomicProgram|TestBeginSQLTransaction)' -count=1
