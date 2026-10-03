#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^(TestExecuteClusterWriteCommitDurable|TestClusterWriteCommitDecision)' -count=1
