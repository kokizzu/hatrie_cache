#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatBackup -run '^TestMZ006' -count=1
