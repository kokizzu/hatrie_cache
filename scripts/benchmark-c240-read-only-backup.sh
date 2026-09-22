#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^$' -bench 'BenchmarkReadOnlyBackup(Attachment|RestoreAndLoad)$' -benchmem -count=5
