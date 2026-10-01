#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology -run 'TestDurableMembership|TestMembership' -count=1
