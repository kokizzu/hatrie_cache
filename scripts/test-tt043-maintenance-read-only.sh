#!/bin/sh
set -eu

go test ./hat/hatCache -run 'Test(CacheGRPCServer|Monitoring|Maintenance).*MaintenanceReadOnly|TestMaintenanceReadOnlyDefaultsOff' -count=1
go test ./cmd/hatrie-cache -run 'TestParseConfigMaintenanceReadOnlyFlagAndFile' -count=1
