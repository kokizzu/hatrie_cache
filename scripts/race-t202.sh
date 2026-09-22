#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatTopology -run 'TestElectionStoreRun' -count=1
