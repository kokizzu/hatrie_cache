#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestBackupBundle(Selects|Rejects)' -count=1
go test ./hat/hatCache -run 'TestBackupBundle(Selects|Rejects)' -race -count=1
go vet ./hat/hatCache ./hat/hatBackup
