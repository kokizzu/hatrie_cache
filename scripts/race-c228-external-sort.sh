#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run '^TestC228ExternalSortPreservesStableOrderAcrossRuns$' -count=1
