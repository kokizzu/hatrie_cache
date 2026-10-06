#!/usr/bin/env bash
set -euo pipefail

test -f MG14_SINK_DELIVERY_RETRY_QUEUE.md
rg -q '^## M-G14 Sink Delivery Retry Queue$' BENCHMARK.md
rg -q 'M-G14 Sink Delivery Retry Queue' BENCHMARK.md
rg -q 'MG14_SINK_DELIVERY_RETRY_QUEUE.md' README.md
rg -q 'SinkRetryQueue' ADOPTED_QUERY_ENGINE_IDEAS.md
rg -q 'M-G14' INSPIRATION.md
printf '%s\n' 'M-G14 documentation links and benchmark section verified.'
