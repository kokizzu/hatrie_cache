#!/usr/bin/env bash
set -euo pipefail
go test ./hat/hatBackup -run 'TestC240' -count=1
