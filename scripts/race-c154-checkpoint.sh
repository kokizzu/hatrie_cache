#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSchema -run 'TestRollingSchemaCheckpoint' -count=1
