#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run 'TestReplicaRecovery|TestSnapshotWALBootstrap|TestPlanReplicaBootstrap|TestReplicaBootstrapWorkflow' -count=1
