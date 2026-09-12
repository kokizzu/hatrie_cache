#!/bin/sh
set -eu

go test -race ./hat/hatCache ./cmd/hatrie-cache -run 'Test(CacheGRPCServer|Monitoring|Maintenance).*MaintenanceReadOnly|TestMaintenanceReadOnlyDefaultsOff|TestParseConfigMaintenanceReadOnlyFlagAndFile' -count=1
