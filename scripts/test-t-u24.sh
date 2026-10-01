#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^TestTU24' -count=1
