#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestCH019' -count=1
