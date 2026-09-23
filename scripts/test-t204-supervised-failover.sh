#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run '^TestElectionStore(Supervised|OperatorOverride)' -count=1
