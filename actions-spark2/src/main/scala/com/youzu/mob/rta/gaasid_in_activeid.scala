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


    //--插入30天内包名不为空且ieid/oiid任一不为空的活跃数据
    dw_active_app_all_rta_table(spark)
    println("step1: dw_install_app_all_rta_table 执行完毕")

    //RTA,7天设备池与一个月活跃应用取交集
    dw_gaasid_in_activeid_table(spark)
    println("step5: dw_gaasid_in_installid_table 执行完毕")

    spark.stop()
  }

  def dw_active_app_all_rta_table(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_ACTIVE_APP_ALL_RTA
         |     select ieid,oiid,apppkg,day
         |     from $DWD_PV_SEC_DI
         |     where day >=date_format(date_sub(current_date,31),'yyyyMMdd')
         |     and concat_ws('',ieid,oiid)!=''
         |     and apppkg is not null and trim(apppkg)!=''
         |     group by ieid,oiid,apppkg,day
         |     union
         |     select ieid,oiid,apppkg,day
         |     from $DWD_MDATA_NGINX_PV_DI
         |     where day >=date_format(date_sub(current_date,31),'yyyyMMdd')
         |     and concat_ws('',ieid,oiid)!=''
         |     and apppkg is not null and trim(apppkg)!=''
         |     group by ieid,oiid,apppkg,day
         |     union
         |     select ieid,oiid,pkg as apppkg,day
         |     from $DWD_PKG_RUNTIMES_SEC_DI
         |     where day >=date_format(date_sub(current_date,31),'yyyyMMdd')
         |     and concat_ws('',ieid,oiid)!=''
         |     and pkg is not null and trim(pkg)!=''
         |     group by ieid,oiid,pkg,day
         |""".stripMargin)
  }

  def dw_gaasid_in_activeid_table(spark: SparkSession) {
    //mob月活数据与rta设备池数据取交集
    spark.sql(
      s"""
         |insert overwrite table $DW_GAASID_IN_ACTIVEID
         |select a.ieid, a.oiid, b.apppkg,b.day
         |from $DW_GAASID_FINAL a
         |join (
         |      select ieid, apppkg,day
         |      from $DW_ACTIVE_APP_ALL_RTA
         |      where ieid is not null and trim(ieid)!=''
         |      group by ieid,apppkg,day
         |) b on a.ieid=b.ieid
         |group by a.ieid,a.oiid,b.apppkg,b.day
         |union
         |select a.ieid, a.oiid, b.apppkg,b.day
         |from $DW_GAASID_FINAL a
         |join (
         |      select oiid, apppkg,day
         |      from $DW_ACTIVE_APP_ALL_RTA
         |      where oiid is not null and trim(oiid)!=''
         |      group by oiid,apppkg,day
         |) b on a.oiid=b.oiid
         |group by a.ieid,a.oiid,b.apppkg,b.day
         |""".stripMargin
    )
  }
}
