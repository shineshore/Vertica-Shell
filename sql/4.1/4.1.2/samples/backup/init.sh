#!/bin/bash

#parameters

curDir=$(pwd)
scriptDir=$(cd "$(dirname $0)"; pwd)

vbr.py --task init --config-file ${curDir}/vbr.ini
