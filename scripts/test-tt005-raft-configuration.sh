#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run 'TestTT005RaftConfiguration' -count=1
