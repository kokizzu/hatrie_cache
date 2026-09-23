#!/usr/bin/env bash
set -euo pipefail

test -f CH044_EXPORTABLE_TRACE.md
rg -n 'NewOTLPHTTPExporter|ExportOpenTelemetry|MaxPayloadBytes|CH044_EXPORTABLE_TRACE' \
  CH044_EXPORTABLE_TRACE.md README.md
