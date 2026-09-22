#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(TestC230|TestCHG42|TestSQLClusterAdmission)'
