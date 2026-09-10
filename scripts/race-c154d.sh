#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSchema -run 'RollingSchemaDeploymentRun' -count=1
