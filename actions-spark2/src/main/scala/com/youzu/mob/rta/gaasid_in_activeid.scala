package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object gaasid_in_activeid {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("gaasid_in_activeid")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //从pv与mdata表，取oiid/ieid，apppkg维度的月活
    spark.sql(
      s"""
        |insert overwrite table $OIID_IEID_ACTIVE_MONTH
        |select 'oiid' as idtype,oiid as idvalue,apppkg,day
        |from(
        |     select oiid,apppkg,day
        |     from $DWD_PV_SEC_DI
        |     where day >=date_format(date_sub(current_date,31),'yyyyMMdd')
        |     and oiid is not null and trim(oiid)!='' and apppkg is not null and trim(apppkg)!=''
        |     group by oiid,apppkg,day
        |     union
        |     select oiid,apppkg,day
        |     from $DWD_MDATA_NGINX_PV_DI
        |     where day >=date_format(date_sub(current_date,31),'yyyyMMdd')
        |     and oiid is not null and trim(oiid)!='' and apppkg is not null and trim(apppkg)!=''
        |     group by oiid,apppkg,day
        |     union
        |     select oiid,pkg as apppkg,day
        |     from $DWD_PKG_RUNTIMES_SEC_DI
        |     where day >=date_format(date_sub(current_date,31),'yyyyMMdd')
        |     and oiid is not null and trim(oiid)!='' and pkg is not null and trim(pkg)!=''
        |     group by oiid,pkg,day
        |)bb
        |group by oiid,apppkg,day
        |union
        |select  'ieid' as idtype, ieid as idvalue,apppkg,day
        |from(
        |     select ieid,apppkg,day
        |     from $DWD_PV_SEC_DI
        |     where day >=date_format(date_sub(current_date,31),'yyyyMMdd')
        |     and ieid is not null and trim(ieid)!='' and apppkg is not null and trim(apppkg)!=''
        |     group by ieid,apppkg,day
        |     union
        |     select ieid,apppkg,day
        |     from $DWD_MDATA_NGINX_PV_DI
        |     where day >=date_format(date_sub(current_date,31),'yyyyMMdd')
        |     and ieid is not null and trim(ieid)!='' and apppkg is not null and trim(apppkg)!=''
        |     group by ieid,apppkg,day
        |     union
        |     select ieid,pkg as apppkg,day
        |     from $DWD_PKG_RUNTIMES_SEC_DI
        |     where day >=date_format(date_sub(current_date,31),'yyyyMMdd')
        |     and ieid is not null and trim(ieid)!='' and pkg is not null and trim(pkg)!=''
        |     group by ieid,pkg,day
        |)bb
        |group by ieid,apppkg,day
        |""".stripMargin)

    //mob月活数据与rta设备池数据取交集
    spark.sql(
      s"""
        |insert overwrite table $DW_GAASID_IN_ACTIVEID
        |select idtype, idvalue,apppkg,day
        |from (
        |select idtype, idvalue,apppkg,day
        |from $OIID_IEID_ACTIVE_MONTH
        |where idtype='ieid'
        |) a
        |left semi join $DW_GAASID_FINAL b
        |on a.idvalue=b.ieid
        |union
        |select idtype, idvalue,apppkg,day
        |from (
        |select idtype, idvalue,apppkg,day
        |from $OIID_IEID_ACTIVE_MONTH
        |where idtype='oiid'
        |) c
        |left semi join $DW_GAASID_FINAL d
        |on c.idvalue=d.oiid
        |""".stripMargin)

    spark.stop()
  }
}
