#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication -run 'TestConflictEventLog' -count=1
