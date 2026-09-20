#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run 'TestTU038' -count=1
