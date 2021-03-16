#!/bin/bash
set -x -e
: '
@owner:guanyt
@describe: 利用每日的增量，获取到新的标签，最新的city_level,city_level_1001
@projectName:MOBDI
'

:<<!
@parameters
@day:传入日期参数,为脚本运行日期(重跑不同)
!
day=$1

source /home/dba/mobdi_center/sbin/mobdi/tag/base_tag/init_source_props.sh


##input
device_applist_new=${dim_device_applist_new_di}
#mapping

#output
label_house_score_di=${label_l1_house_score_di}

hive -v -e "
SET hive.merge.mapfiles=true;
SET hive.merge.mapredfiles=true;
set mapred.max.split.size=250000000;
set mapred.min.split.size.per.node=128000000;
set mapred.min.split.size.per.rack=128000000;
set hive.merge.smallfiles.avgsize=250000000;
set hive.merge.size.per.task = 250000000;
insert overwrite table $label_house_score_di partition(day='$day')
select device, 1.0 as label
from $device_applist_new
where day = '$day'
and pkg in (
'air.com.h1j.diy',
'android.decorate.cases.jiajuol.com',
'android.decorate.gallery.jiajuol.com',
'cn.org.camib.www2.WeBrowser.gxfcjc',
'com.aiyiqi.galaxy',
'com.blej.chaojiguanliyuan',
'com.bzw.zxbzw_app',
'com.cs.decoration',
'com.cyjysoft.asfapp',
'com.djiaju.decoration',
'com.e1858.building',
'com.example.decorate',
'com.fuwo.ifuwo',
'com.goldmantis.app.jia',
'com.house.makebudget',
'com.houzz.app',
'com.hudee.mama4f9b7826421ddccfdc7194e8',
'com.huizhuang.hz',
'com.huizhuang.zxsq',
'com.ikongjian',
'com.jiajuol.decoration',
'com.juguo.kuma',
'com.lingduo.acorn',
'com.muniao',
'com.qunhe.designhome',
'com.qunhe.rendershow',
'com.redstar.mainapp',
'com.soufun.decoration.app',
'com.suryani.jiagallery',
'com.tbs.tobosutype',
'com.to8to.assistant.activity',
'com.to8to.housekeeper',
'com.to8to.tuku',
'com.to8to.wireless.designroot',
'com.to8to.wireless.to8to',
'com.to8to.zxtyg',
'com.vanlian.client',
'com.xigua.moveassistant',
'com.xiuwojia.xiuwojia',
'com.xtuan.meijia',
'com.xyj.zxxyj_app',
'com.yidian.house',
'com.yidoutang.app',
'com.youwe.dajia',
'com.zhaidou',
'com.zhishan.wawu',
'com.zhuke.app',
'net.yinwan.payment',
'prancent.project.rentalhouse.app'
)
group by device;
"
