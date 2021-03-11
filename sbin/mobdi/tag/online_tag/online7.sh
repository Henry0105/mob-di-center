#!/usr/bin/env bash

day=$1
timewindow=$2
path="$(cd "`dirname "$0"`"; pwd)"
sh ${path}/online_tool.sh ${day} ${timewindow} 7 0
