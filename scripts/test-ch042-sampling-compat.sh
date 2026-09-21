#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestSQLTableSample|TestSampleSQLRows|TestCH042StorageAwareSample)' -count=1
