#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM065m' -count=1
