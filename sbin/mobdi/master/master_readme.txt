
master中脚本为数据集市中 明细，轻汇总数据
#脚本目录结构定义规则
1.按照主题模块
2.按照数据分层
ps：
master/主题1/dwd/script1
            dws/script2
            ads/script3
master/主题2/dwd/script11
            dws/script22
            ads/script33

#脚本命名规则
表命名规范：

ODS（etl层）：ods_[业务库名]_{业务库原始表名}[_delta]

DWD（master明细数据层）：dwd_{主题缩写}_{业务过程缩写}[_自定义标签缩写]_{单分区增量全量标识}

DWM（master中间层）：dwm_{主题缩写}_{业务过程缩写}[_自定义标签缩写]_{单分区增量全量标识}

DWS（master汇总层）：dws_{数据域缩写}[_自定义标签缩写]_{刷新周期标识}

ADS|RP|APP（report应用层）：ads_ [_业务应用缩写][_维度][_自定义标签缩写]_{刷新周期标识}

DIM（维度表）：dim_{维度定义}



-------------------------
目录含义:
device_app_active_info:设备活跃相关脚本

device_app_install_and_unstall_info：设备安装/卸载app行为分析脚本

device_bash_info : 设备基础信息处理相关脚本

device_location_info：设备地理位置信息分析处理脚本

device_sns_info：通讯录相关数据处理脚本


