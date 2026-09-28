#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatRate -count=1
