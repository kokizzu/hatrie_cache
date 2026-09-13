#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestSQLRowBinaryStream' -count=1
go test ./hat/hatCache -run 'TestMonitoringSQLRoute.*RowBinary|TestSQLClientReadsRowBinaryStream' -count=1
