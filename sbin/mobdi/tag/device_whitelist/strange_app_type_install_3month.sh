#!/bin/sh

day=$1
p3months=`date -d "$day -90 day" +%Y%m%d`

source /home/dba/mobdi_center/conf/hive_db_tb_topic.properties

#input
#dws_device_install_app_re_status_di=dm_mobdi_topic.dws_device_install_app_re_status_di
#out
device_strange_app_type_install_3month=${dm_mobdi_tmp}.device_strange_app_type_install_3month

hive -e"
SET hive.exec.parallel=true;
SET hive.exec.parallel.thread.number=15;
SET hive.auto.convert.join=true;
SET hive.map.aggr=true;
SET hive.merge.mapfiles=true;
set hive.merge.size.per.task=256000000;
set hive.merge.smallfiles.avgsize=256000000;

insert overwrite table $device_strange_app_type_install_3month
select device, pkg, strange_type
from 
(
  select device, pkg,
  case 
    when pkg in ('com.chuangdian.ipjlsdk','com.chuangdian.ipjl2','com.huashidai','com.mimi6775','com.chuangdian.ipjl2') then '代理ip'
    when pkg in ('com.wx.mockgps','com.rong.xposed.fakelocation','com.wifi99.android.locationcheater','io.xudwoftencentmm','org.proxydroid','top.a1024bytes.mockloc.ca.pro','com.txy.anywheren','com.txy.anywhere694','com.deniu.multi','com.lexa.fakegps','com.qgwapp.shadowside','com.lerist.fakelocation','net.superal') then '篡改gps'
    when pkg in ('com.example.myxposed','com.soft.apk008v','com.sollyu.xposed.hook.model','com.xtools.x008','com.fangwei.pj008','com.aso.manager','com.mhz.changeinfo','com.soft.apk008Tool','com.hq.mobilemodifier','org.imei.mtk65xx','com.soft.apk008w','com.zhy.fakedev') then '改机'
  end as strange_type
  from $dws_device_install_app_re_status_di
  where day >= '$p3months' and day <= '$day' and refine_final_flag = 1
  and pkg in 
  ('com.chuangdian.ipjlsdk',
  'com.chuangdian.ipjl2',
  'com.huashidai',
  'com.mimi6775',
  'com.chuangdian.ipjl2'
  'com.wx.mockgps',
  'com.rong.xposed.fakelocation',
  'com.wifi99.android.locationcheater',
  'io.xudwoftencentmm',
  'org.proxydroid',
  'top.a1024bytes.mockloc.ca.pro',
  'com.txy.anywheren',
  'com.txy.anywhere694',
  'com.deniu.multi',
  'com.lexa.fakegps',
  'com.qgwapp.shadowside',
  'com.lerist.fakelocation',
  'net.superal'
  'com.example.myxposed',
  'com.soft.apk008v',
  'com.sollyu.xposed.hook.model',
  'com.xtools.x008',
  'com.fangwei.pj008',
  'com.aso.manager',
  'com.mhz.changeinfo',
  'com.soft.apk008Tool',
  'com.hq.mobilemodifier',
  'org.imei.mtk65xx',
  'com.soft.apk008w',
  'com.zhy.fakedev')
) as a 
group by device, pkg, strange_type
"