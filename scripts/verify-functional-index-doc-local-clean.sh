#!/usr/bin/env bash
set -euo pipefail

test -s FUNCTIONAL_INDEX.md
grep -F 'hatDataStructure.FunctionalIndex' FUNCTIONAL_INDEX.md >/dev/null
grep -F 'make benchmark-functional-index-local-clean' FUNCTIONAL_INDEX.md >/dev/null
grep -F '54.1x faster' FUNCTIONAL_INDEX.md >/dev/null
