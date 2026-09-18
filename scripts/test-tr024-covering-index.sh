#!/usr/bin/env bash
set -euo pipefail

go test -tags tr024covering ./hat/hatSchema -run 'TestMaterializedSource(BuildsAndMaintainsCoveringIndex|SQLResolverUsesCoveringIndex)$' -count=1
