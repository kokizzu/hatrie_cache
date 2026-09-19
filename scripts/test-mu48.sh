#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatPipeline -run '^Test(StartWithHealthPolicy|QuarantineConnector|ConnectorHealthPolicy)'
