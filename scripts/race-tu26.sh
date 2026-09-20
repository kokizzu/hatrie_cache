#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TU26|SQLIndexHint'
