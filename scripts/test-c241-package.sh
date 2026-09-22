#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^(TestC241|TestIncrementalBackupRepository|TestBackupRepository|TestRehearseRestoreRestoresIncrementalRepository|TestRestoreBackupRepositoryResumeRepairsStaleFiles)' -count=1
