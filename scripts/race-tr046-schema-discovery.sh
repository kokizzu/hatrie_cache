#!/usr/bin/env bash
set -eu
go test -race ./hat/hatSchema -run 'TestTR046SchemaDiscovery' -count=1
