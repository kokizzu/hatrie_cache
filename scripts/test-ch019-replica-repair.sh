#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^TestCH019' -count=1
