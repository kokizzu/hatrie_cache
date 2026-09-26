#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestMZ009ValidityPartitionPruning$' -count=1
