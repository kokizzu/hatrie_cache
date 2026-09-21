#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench '^BenchmarkC240Backup(RestoreAndQuery|AttachmentOpenAndQuery)$' -benchtime=5x -count=5
