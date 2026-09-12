#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run 'TestPlanBackupChain' -count=1
