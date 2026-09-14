#!/usr/bin/env bash
set -euo pipefail

go test ./cmd/hatrie-cli -run '^TestRunRestoreRehearsalVerifiesBackupPath$' -count=1 -v
