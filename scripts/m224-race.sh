#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestM224' -count=1
