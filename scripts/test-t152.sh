#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run '^TestReplicationChaos' -count=1
