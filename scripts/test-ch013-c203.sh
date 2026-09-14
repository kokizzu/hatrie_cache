#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestCH013' -count=1
go test ./hat/hatCache -run 'TestCH013' -count=1
