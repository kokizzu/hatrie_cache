#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run 'Test(ClusterWriteCommitCoordinatorFileStore|ExecuteClusterWriteCommitWithStateStore)' -count=1
