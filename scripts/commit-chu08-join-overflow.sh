#!/bin/sh
set -eu

git diff --cached --check
git commit -m "Add explicit join overflow policies [skip ci]"
