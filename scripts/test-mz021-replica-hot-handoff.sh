#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestMZ021ReplicaHotHandoff' -count=1
