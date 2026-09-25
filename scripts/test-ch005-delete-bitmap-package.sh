#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMerkle ./hat/hatSql -run '^TestCH005' -count=1
go test ./hat/hatMerkle ./hat/hatSql -count=1
