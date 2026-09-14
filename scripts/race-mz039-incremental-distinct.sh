#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^(TestMZ039|TestIncrementalDistinct)' -count=1
