#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatWorkload -run 'TestAdmission' -count=1
