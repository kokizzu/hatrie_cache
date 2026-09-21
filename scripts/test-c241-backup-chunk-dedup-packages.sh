#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatBackup ./hat/hatCache -count=1
