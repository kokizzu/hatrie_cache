#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestM242' -race -count=1
