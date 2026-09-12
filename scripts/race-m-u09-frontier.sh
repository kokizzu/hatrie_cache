#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
go test -C "$root" -race ./hat/hatPipeline -run '^TestFrontierRegistry' -count=1
