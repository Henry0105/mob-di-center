package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object gaasid_in_installid {

  def main(args: Array[String]): Unit = {
    // 1. 初始化
    val spark = SparkSession
      .builder()
      .appName("gaasid_in_installid")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //传入日期参数
    val insert_day = args(0)

    //--在装应用汇总表
    //--插入30天内包名不为空且ieid/oiid任一不为空的在装应数据
    dw_install_app_all_rta_table(spark, insert_day)
    println("step1: dw_install_app_all_rta_table 执行完毕")

    //RTA,7天设备池与一个月在装应用清洗表取交集
    dw_gaasid_in_installid_table(spark, insert_day)
    println("step5: dw_gaasid_in_installid_table 执行完毕")
    spark.stop()
  }

  def dw_install_app_all_rta_table(spark: SparkSession, insert_day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_INSTALL_APP_ALL_RTA partition(day='$insert_day')
         |  select
         |  ieid,
         |  oiid,
         |  pkg
         |from
         |    $DWD_LOG_DEVICE_INSTALL_APP_ALL_INFO_SEC_DI
         |where day>=date_format(date_sub(current_date,31),'yyyyMMdd')
         |    and pkg is not null and trim(pkg)!='' and substr(firstinstalltime,-3)!='000'
         |    and ((ieid is not null and trim(ieid)!='') or (oiid is not null and trim(oiid)!=''))
         |group by ieid,oiid,pkg
         |""".stripMargin)
  }

  def dw_gaasid_in_installid_table(spark: SparkSession, insert_day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_GAASID_IN_INSTALLID
         |    select
         |    a.ieid,
         |    a.oiid,
         |    b.pkg
         |    from $DW_GAASID_FINAL a
         |        join
         |        (
         |          select ieid, pkg
         |          from $DW_INSTALL_APP_ALL_RTA
         |          where day='$insert_day'and ieid is not null and trim(ieid)!=''
         |          group by ieid,pkg
         |         ) b on a.ieid=b.ieid
         |    group by a.ieid,a.oiid,b.pkg
         |union
         |    select a.ieid, a.oiid, b.pkg
         |    from $DW_GAASID_FINAL a
         |        join
         |        (
         |          select oiid, pkg
         |          from $DW_INSTALL_APP_ALL_RTA
         |          where day='$insert_day'and oiid is not null and trim(oiid)!=''
         |          group by oiid,pkg
         |         ) b on a.oiid=b.oiid
         |    group by a.ieid,a.oiid,b.pkg
         |""".stripMargin
    )
  }
}
