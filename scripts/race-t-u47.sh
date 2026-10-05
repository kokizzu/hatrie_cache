#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatPeer -run '^Test(ConnectionPoolCloseCancelsActiveHandlerContext|CompactPeerSessionCallCancellationInterruptsBlockedWrite|CompactPeerSessionCallTemplateCancellationInterruptsBlockedWrite|CompactPeerSessionWriteCancellationBeatsLaterDeadline|TU47DisabledCancellationRepeated)$' -count=1 -timeout=2m
