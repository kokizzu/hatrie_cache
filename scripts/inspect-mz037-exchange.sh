#!/bin/sh
set -eu

rg -n -i 'exchange|worker|parallel|goroutine|channel|batch' hat/hatSql/*.go hat/hatCache/*.go hat/hatPipeline/*.go hat/hatDataStructure/*.go
