#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestTT003' -count=1
