#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run '^TestC239SnapshotCompactionMetrics' -count=1
