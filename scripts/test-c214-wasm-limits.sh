#!/usr/bin/env bash
set -euo pipefail

go test ./hat/hatCache -run 'TestSQLWASMRegistryUsesMemoryLimitAndKeepsTimeoutOptIn|TestSQLWASMRegistryRejectsModuleAboveMemoryLimit|TestSQLWASMFunctionExecutionTimeoutStopsInfiniteLoop|TestSQLWASMFunctionStillRunsWithExplicitMemoryLimit' -count=1
