#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatSql -run 'TestM065DifferentialRowNumberLag' -count=1
