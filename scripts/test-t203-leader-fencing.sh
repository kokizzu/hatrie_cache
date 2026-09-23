#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'Test(RejectNonLeaderWrite|ExecuteCacheCommand)' -count=1
