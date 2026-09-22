#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestC230|TestCHG42|TestSQLClusterAdmission)'
