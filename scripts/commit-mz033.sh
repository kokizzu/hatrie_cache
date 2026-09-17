#!/bin/sh
set -eu

git diff --cached --check
git commit -m 'Add automatic dataflow index advisor [skip ci]'
