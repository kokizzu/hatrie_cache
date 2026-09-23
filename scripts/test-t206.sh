#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run 'TestPlanReplicaBootstrap|TestReplicaBootstrapWorkflow' -count=1
