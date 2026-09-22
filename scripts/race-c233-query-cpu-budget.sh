#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestC233' -count=1
