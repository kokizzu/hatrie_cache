#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run 'TestPlanReplicaBootstrap|TestReplicaBootstrapWorkflow|TestSnapshotWALBootstrap' -count=1
