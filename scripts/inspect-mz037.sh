#!/bin/sh
set -eu

rg -n -i 'exchange|worker.?local|dataflow|parallel.*batch|batch.*parallel' hat/hatSql hat/hatCache hat/hatPipeline hat/hatDataStructure README.md *.md
