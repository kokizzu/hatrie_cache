#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run 'TestExecuteClusterWriteCommit' -count=1
