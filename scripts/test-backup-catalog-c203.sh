#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run 'TestBackupManifestCatalog' -count=1
go test ./hat/hatBackup -count=1
