#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestMU040' -race -count=1
