#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'Test(Create(IncrementalBackupRepository|BackupBundle)WithContextCancellation|IncrementalBackupRepository|BackupRepository|BackupBundle|Restore)' -count=1
