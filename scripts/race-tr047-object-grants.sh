#!/usr/bin/env bash
set -euo pipefail

go test -race -count=1 ./hat/hatAuth ./hat/hatCache -run '^Test(Policy(ObjectScopedRole|ObjectGrantRequiresObjectAndPreservesLegacyRules)|MonitoringHandlerEnforcesObjectGrant|MonitoringSQLRowBinaryImport|CacheGRPCServerEnforcesObjectGrant)'
