#!/bin/sh
set -eu

git diff --cached --check
git commit -m 'Add arrangement cost model [skip ci]'
