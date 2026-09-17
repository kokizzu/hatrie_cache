#!/bin/sh
set -eu

git status --short --branch
git log -1 --oneline
