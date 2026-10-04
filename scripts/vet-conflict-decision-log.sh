#!/bin/sh
set -eu

go vet ./hat/hatReplication "$@"
