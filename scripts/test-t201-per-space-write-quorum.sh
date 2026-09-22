#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestWriteQuorumPolicy|TestExecuteCacheCommandAppliesPerSpaceWriteQuorum' -count=1
