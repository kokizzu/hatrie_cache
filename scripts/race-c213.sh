#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestC213' -count=1
