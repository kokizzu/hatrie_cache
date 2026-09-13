#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run '^TestObjectStoreEncrypted' -count=1
