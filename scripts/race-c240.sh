#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatBackup ./hat/hatCache -run 'Test(ReadOnlyBackupAttachment|OpenPebbleStoreReadOnly)' -count=1
