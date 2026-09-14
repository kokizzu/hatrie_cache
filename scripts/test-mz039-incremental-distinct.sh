#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^(TestMZ039|TestIncrementalDistinct)' -count=1
