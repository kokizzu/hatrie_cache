#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatBackup -run 'TestCopyRestoreFiles' -count=1
