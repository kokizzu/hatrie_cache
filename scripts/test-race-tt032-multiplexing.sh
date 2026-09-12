#!/bin/sh
set -eu

go test -race ./hat/hatCache -run '^TestCacheGRPCServer(CommandStreamCorrelatesMultiplexedResponses|CommandStreamRejectsMixedRequestIDModes|NormalizesCommandStreamWorkers)$' -count=1
