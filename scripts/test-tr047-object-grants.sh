#!/usr/bin/env bash
set -euo pipefail

go test -count=1 ./hat/hatAuth ./hat/hatCache -run '^Test(Policy(ObjectScopedRole|ObjectGrantRequiresObjectAndPreservesLegacyRules)|MonitoringHandlerEnforcesObjectGrant|MonitoringSQLRowBinaryImport|CacheGRPCServerEnforcesObjectGrant)'
