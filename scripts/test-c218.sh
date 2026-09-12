#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestC218' -count=1
