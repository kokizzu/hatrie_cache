#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatBackup ./hat/hatCache -count=1
