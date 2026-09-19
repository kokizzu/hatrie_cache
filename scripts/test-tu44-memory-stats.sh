#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatMemoryStats -count=1
