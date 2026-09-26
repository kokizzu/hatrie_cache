#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestT042ParallelReplay' -count=1
