#!/bin/sh
set -eu
rg -n 'CH-U23|AsyncInsertQueueRegistry|/api/async-inserts|async insert queue' README.md PRODUCT_IDEA_GAPS.md ADOPTED_QUERY_ENGINE_IDEAS.md INSPIRATION.md BENCHMARK.md CHU23_ASYNC_INSERT_QUEUE.md
