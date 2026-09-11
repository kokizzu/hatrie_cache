#!/usr/bin/env bash
set -euo pipefail

test -f CDC_ENVELOPES.md
rg -F 'MZ-015' ENGINE_IDEAS.md ADOPTED_QUERY_ENGINE_IDEAS.md BENCHMARK.md CDC_ENVELOPES.md
rg -F 'hatSql.NormalizeCDCEnvelope' README.md CDC_ENVELOPES.md ADOPTED_QUERY_ENGINE_IDEAS.md
rg -F 'hatSql.DecodeCDCEnvelopeJSON' README.md CDC_ENVELOPES.md
rg -F 'BENCHMARK.md#mz-015-cdc-envelope-normalization' README.md ADOPTED_QUERY_ENGINE_IDEAS.md CDC_ENVELOPES.md
rg -F 'make test-mz015-cdc' CDC_ENVELOPES.md
rg -F 'make benchmark-mz015-cdc' CDC_ENVELOPES.md BENCHMARK.md
printf '%s\n' 'MZ-015 documentation links, API references, benchmark references, and commands verified.'
