#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestRehearseRestoreComparesRecoveredStateChecksums$' -count=1
