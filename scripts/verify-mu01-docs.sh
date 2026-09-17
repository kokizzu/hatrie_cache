#!/bin/sh
set -eu

test -f MU01_DURABLE_CONNECTOR_STATE.md
rg -q '^\| M-U01 .*MU01_DURABLE_CONNECTOR_STATE.md' PRODUCT_IDEA_GAPS.md
rg -q '^## M-U01 Durable Connector Lifecycle State' BENCHMARK.md
rg -q 'Materialize-style durable connector lifecycle checkpoints' README.md
rg -q 'M106 Durable connector lifecycle checkpoints' INSPIRATION.md
rg -q '^## Materialize M-U01: Durable Connector Lifecycle State' ADOPTED_QUERY_ENGINE_IDEAS.md
