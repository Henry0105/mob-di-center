#!/bin/bash
cd `dirname $0`
echo "加载hive表变量..."
source ./hive_db_tb_dashboard.properties
source ./hive_db_tb_master.properties
source ./hive_db_tb_mobdi_mapping.properties
source ./hive_db_tb_report.properties
source ./hive_db_tb_sdk_mapping.properties
source ./hive_db_tb_topic.properties
echo "hive表变量加载完毕"
