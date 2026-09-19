#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatWorkload -run 'TestAdmission' -count=1
