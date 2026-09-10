#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatTopology ./hat/hatCache -run 'TopologyCommit|TopologyConsensus' -count=1
