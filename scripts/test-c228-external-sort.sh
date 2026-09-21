#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run '^TestC228ExternalSortPreservesStableOrderAcrossRuns$' -count=1
