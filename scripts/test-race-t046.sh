#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatCache -run 'Test(Create(IncrementalBackupRepository|BackupBundle)WithContextCancellation|IncrementalBackupRepository|BackupRepository|BackupBundle|Restore)' -count=1
