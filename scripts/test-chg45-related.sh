#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'Test(CH045|MZ044)' -count=1
