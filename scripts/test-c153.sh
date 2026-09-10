#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology ./hat/hatCache -run 'TopologyCommit|TopologyConsensus' -count=1
