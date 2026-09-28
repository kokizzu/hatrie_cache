#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatWorkload -count=1
