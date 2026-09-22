#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestC233' -count=1
