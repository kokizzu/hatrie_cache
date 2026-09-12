#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestC213' -count=1
