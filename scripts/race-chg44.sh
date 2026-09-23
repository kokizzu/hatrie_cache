#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatTrace ./hat/hatSql -run 'Test(OTLPHTTPExporter|QueryTraceRecorderExportsThroughSpanExporter)'
