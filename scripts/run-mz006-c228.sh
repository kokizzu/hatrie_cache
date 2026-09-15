#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup -run '^TestMZ006' -count=1
