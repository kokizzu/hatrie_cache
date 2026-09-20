#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^TestTU39' -count=1
