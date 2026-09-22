#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^TestT207' -count=1
