#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatTrace ./hat/hatSql -run 'Test(OTLPHTTPExporter|QueryTraceRecorderExportsThroughSpanExporter)'
