#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run '^TestCH022' -count=1
