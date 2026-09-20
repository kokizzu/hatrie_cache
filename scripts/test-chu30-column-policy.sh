#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatStorage -run 'TestRemotePartCacheColumn' -count=1 -v
