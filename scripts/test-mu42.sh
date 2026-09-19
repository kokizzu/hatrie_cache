#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^TestConnectorCheckpoint' -count=1
