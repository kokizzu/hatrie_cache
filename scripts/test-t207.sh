#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run 'TestReplicaRecovery' -count=1
