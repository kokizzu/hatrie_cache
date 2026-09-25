#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMZ035IncrementalMultiset' -count=1
