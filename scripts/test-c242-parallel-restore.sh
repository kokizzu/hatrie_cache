#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run 'TestCopyRestoreFiles' -count=1
