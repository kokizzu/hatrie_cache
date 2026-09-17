#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSql -run 'TestIncrementalProjectionBackfillAtFrontierHandoffsWithoutGap|TestIncrementalProjectionBackfillFailureDoesNotAdvanceCheckpoint|TestIncrementalProjectionBackfillCheckpointFailureKeepsReplayBoundary' -count=1
