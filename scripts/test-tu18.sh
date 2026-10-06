#!/usr/bin/env bash
set -euo pipefail

cache="$PWD/.codex-gocache-tu18"
mkdir -p "$cache"
GOCACHE="$cache" go test -tags tu18 ./hat/hatCache -run 'TestVolatileEngine' -count=1
