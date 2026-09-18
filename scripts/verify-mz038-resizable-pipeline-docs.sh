#!/usr/bin/env bash
set -euo pipefail

test -f MZ038_DYNAMIC_DATAFLOW_WORKER_SCALING.md
rg -q 'MZ-038 Dynamic Dataflow Worker Scaling' MZ038_DYNAMIC_DATAFLOW_WORKER_SCALING.md
rg -q 'MZ038_DYNAMIC_DATAFLOW_WORKER_SCALING.md' README.md
rg -q 'MZ-38.*\[x\]' INSPIRATION_BACKLOG.md
rg -q 'mz-038-dynamic-dataflow-worker-scaling' BENCHMARK.md
rg -q 'ResizablePipeline' ADOPTED_QUERY_ENGINE_IDEAS.md
