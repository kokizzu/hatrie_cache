#!/bin/sh
set -eu

cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
go test ./hat/hatCommand ./hat/hatCache -run '^(TestProtocol|TestMonitoringCommandProtocol)' -count=1
