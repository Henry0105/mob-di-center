#!/bin/bash
conf_path=/home/dba/mobdi_center/conf
echo "加载hive表变量..."
source $conf_path/hive_db_tb_dashboard.properties
source $conf_path/hive_db_tb_master.properties
source $conf_path/hive_db_tb_mobdi_mapping.properties
source $conf_path/hive_db_tb_report.properties
source $conf_path/hive_db_tb_sdk_mapping.properties
source $conf_path/hive_db_tb_topic.properties
echo "hive表变量加载完毕"
