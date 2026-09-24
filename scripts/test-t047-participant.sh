#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^TestClusterWriteCommitParticipant' -count=1
