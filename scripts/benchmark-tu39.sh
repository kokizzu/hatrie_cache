#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatReplication \
  -run '^$' \
  -bench '^BenchmarkTU39(SpaceChangefeedPublish|SpaceChangefeedPublishWithSubscriber)$' \
  -benchmem \
  -benchtime=1s \
  -count=3
