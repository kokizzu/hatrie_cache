#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestM242' -count=1
