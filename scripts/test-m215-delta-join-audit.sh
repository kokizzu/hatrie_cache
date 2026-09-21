#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMZ030' -count=1
