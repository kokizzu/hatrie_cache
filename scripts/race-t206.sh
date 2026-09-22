#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestReplicaJoinAdmission' -count=1
