#!/bin/sh
set -eu

if [ -e .github/workflows/ci.yml ]; then
	echo "GitHub CI workflow is enabled: .github/workflows/ci.yml" >&2
	exit 1
fi

echo "GitHub push/pull-request CI workflow is disabled"
