#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run '^TestTU38' -count=1
