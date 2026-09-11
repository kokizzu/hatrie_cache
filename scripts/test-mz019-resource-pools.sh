#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatSql -run 'TestNamespaceQueryGovernorComputePool|TestNamespaceQueryGovernorRejectsInvalidComputePoolLimits' -count=1
