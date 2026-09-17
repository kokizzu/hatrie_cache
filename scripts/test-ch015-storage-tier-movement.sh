#!/bin/sh
set -eu

go test ./hat/hatStorage -run '^TestCH015StorageTierMove'
