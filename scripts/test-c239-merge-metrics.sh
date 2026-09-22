#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestC239PartMergeMetrics' -count=1
