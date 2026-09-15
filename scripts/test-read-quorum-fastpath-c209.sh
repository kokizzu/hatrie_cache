#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^TestExecuteReadQuorumSingleNode' -count=1
