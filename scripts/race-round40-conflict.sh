#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatReplication -run 'TestConflictEventLog' -count=1
