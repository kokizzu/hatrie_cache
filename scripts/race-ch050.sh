#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql ./hat/hatCache -count=1
