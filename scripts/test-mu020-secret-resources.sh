#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatAuth -run '^TestResourceRegistry' -count=1
