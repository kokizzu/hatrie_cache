#!/usr/bin/env bash
set -euo pipefail

cache=$(mktemp -d /tmp/hatrie-cache-m045-verify.XXXXXX)
trap 'rm -rf "$cache"' EXIT
GOCACHE="$cache" bash ./scripts/verify-mu032-function-capabilities.sh
