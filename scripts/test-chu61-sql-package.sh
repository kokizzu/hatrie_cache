#!/usr/bin/env bash
set -euo pipefail

GOCACHE="$PWD/.gocache-package" go test ./hat/hatSql -count=1
