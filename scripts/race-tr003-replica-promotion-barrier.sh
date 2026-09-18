#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestTR003ReplicaPromotionBarrier' -count=1
