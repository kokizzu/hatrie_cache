#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatTopology -run 'TestTT005RaftConfiguration' -count=1
