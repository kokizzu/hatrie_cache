#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestC213SQLHashJoinUsesTypedIndex$' -count=1
