#!/usr/bin/env bash
set -euo pipefail

go test -race ./hat/hatSchema -run 'TestMaterializedSource(BuildsAndMaintainsCoveringIndex|SQLResolverUsesCoveringIndex)$' -count=1
