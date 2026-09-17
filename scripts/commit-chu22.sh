#!/bin/sh
set -eu
git diff --cached --check
git commit -m 'Add direct columnar typed-table appends [skip ci]'
