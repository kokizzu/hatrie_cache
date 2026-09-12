#!/bin/sh
set -eu

git fetch origin master
git status --short --branch
git log --oneline --decorate -5 HEAD origin/master
