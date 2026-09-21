#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatBackup ./hat/hatCache -run '^TestC241' -count=1
