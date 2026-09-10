#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSchema -run '^$' -bench 'BenchmarkRollingSchema(ManualTransitions|DeploymentRun)' -benchmem -count=5
