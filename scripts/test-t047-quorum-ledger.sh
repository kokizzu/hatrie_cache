#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run 'TestExecuteClusterWriteCommitWithLedger|TestClusterWriteCommitLedger' -count=1
