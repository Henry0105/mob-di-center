#!/usr/bin/env bash
set -x -e

if [ $# -eq 0 ]; then
    echo "version-build.sh {version}"
    exit -1
fi
PROJECT_VERSION=$1
DATAENGINE_HOME="$(cd "`dirname "$0"`"; pwd)"

cd ${DATAENGINE_HOME}

mvn versions:set -DnewVersion=${PROJECT_VERSION}

mvn versions:commit