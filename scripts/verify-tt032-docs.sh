#!/bin/sh
set -eu

test -f TT032_IPROTO_MULTIPLEXING.md
rg -q 'TT032_IPROTO_MULTIPLEXING\.md' README.md
rg -q '^| TT-031 \| Implemented:' ENGINE_IDEAS.md
rg -q '^| TT-032 \| IProto-style multiplexing \| Partially adopted:' ENGINE_IDEAS.md
rg -q 'tt-032-iproto-style-command-multiplexing' BENCHMARK.md
rg -q 'Multiplexed, 4 workers, 32 requests' BENCHMARK.md
