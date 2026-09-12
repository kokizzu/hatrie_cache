#!/bin/sh
set -eu

go test ./hat/hatCache -run '^TestCacheGRPCServer(CommandStreamCorrelatesMultiplexedResponses|CommandStreamRejectsMixedRequestIDModes|NormalizesCommandStreamWorkers)$' -count=1
