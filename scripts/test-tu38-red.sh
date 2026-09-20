#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run '^TestTU38' -count=1
