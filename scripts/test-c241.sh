#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run 'TestC241' -count=1
