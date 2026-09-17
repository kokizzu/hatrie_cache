#!/bin/sh
set -eu
git diff --cached --check
git commit -m 'Add async insert queue status and flush API [skip ci]'
