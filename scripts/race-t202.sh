#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestT202' -count=1
