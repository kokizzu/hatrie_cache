#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql \
	-run '^Test(NormalizeCDCEnvelope|DecodeCDCEnvelopeJSON)' \
	-count=1
