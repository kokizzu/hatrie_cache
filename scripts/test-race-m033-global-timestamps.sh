#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestGlobalTimestampOracle' -count=20
