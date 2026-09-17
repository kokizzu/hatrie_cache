#!/usr/bin/env bash
set -euo pipefail

exec go test ./hat/hatSql -run 'TestSQLClusterAdmissionPriority|TestSQLClusterAdmissionWorkload' -count=1
