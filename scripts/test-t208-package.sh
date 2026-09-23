#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTopology ./hat/hatReplication -count=1
