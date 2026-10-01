#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run 'TestPlanFailover|TestFailover' -count=1
