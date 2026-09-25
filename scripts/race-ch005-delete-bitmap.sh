#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatMerkle ./hat/hatSql -run '^TestCH005' -count=1
