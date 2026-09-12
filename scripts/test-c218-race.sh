#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestC218' -count=1
