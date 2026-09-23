#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatTopology ./hat/hatReplication -run 'TU208|Anonymous' -count=1
