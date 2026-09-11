#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestBackup(Bundle|Repository)' -count=1
