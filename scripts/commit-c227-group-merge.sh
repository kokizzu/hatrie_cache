#!/bin/sh
set -eu
git diff --cached --check
git commit -m "Bound external group merge memory [skip ci]"
