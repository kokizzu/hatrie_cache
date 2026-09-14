#!/usr/bin/env bash
set -euo pipefail

go test -count=1 ./hat/hatAudit ./hat/hatCache ./cmd/hatrie-cache -run '^Test(AuditLogger|AuditLoggerIsUsableByImporters|AuditAliases|ParseConfig.*AuditSuccessSampleRate|OpenAuditLogIfConfiguredUsesSuccessSampling|MonitoringWrapperPassesAuditSuccessSampleRate)'
