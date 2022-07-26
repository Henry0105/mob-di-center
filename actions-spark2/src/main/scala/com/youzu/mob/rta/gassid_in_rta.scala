package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object gassid_in_rta {
  def main(args: Array[String]): Unit = {
    // 1. 初始化
    val spark = SparkSession
      .builder()
      .appName("gassid_in_rta")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //传入日期参数
    val insert_day = args(0)
    val full_par = args(1)

    //RTA设备池_未添加分类字段
    //从dm_mobdi_master.dwd_gaas_rta_id_data_di中提取出1周内的去重ieid、oiid数据，入结果表
    dw_gaas_id_data_di_rta_table(spark)
    println("step2: dw_gaas_id_data_di_rta_table 执行完毕")

    //更新RTA异常设备池(对应oiid数>2的imei，及对应ieid数>3的oiid均入黑名单，需继承历史黑名单)
    dw_gaasid_blacklist_table(spark, insert_day, full_par)
    println("step3: dw_gaasid_blacklist_table 执行完毕")

    //RTA设备信息池(RTA请求设备信息去重表过滤黑名单，不继承历史数据)
    dw_gaasid_final_table(spark, insert_day)
    println("step4: dw_gaasid_final_table 执行完毕")
    spark.stop()
  }

  def dw_gaas_id_data_di_rta_table(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_GAAS_ID_DATA_DI_RTA
         |select
         |  ieid,
         |  oiid
         |from $DWD_GAAS_RTA_ID_DATA_DI
         |where day >=date_format(date_sub(current_date,8),'yyyyMMdd')
         |group by ieid,oiid
         |""".stripMargin)
  }

  def dw_gaasid_blacklist_table(spark: SparkSession, insert_day: String, full_par: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_GAASID_BLACKLIST partition(day='$insert_day')
         |   select
         |    'ieid' as type,
         |    ieid as idvalue
         |    from $DW_GAAS_ID_DATA_DI_RTA
         |    where ieid is not null and trim(ieid)!='' and oiid is not null and trim(oiid)!=''
         |    group by ieid
         |    having count(1)>2
         |union
         |   select 'oiid' as type, oiid as idvalue
         |    from $DW_GAAS_ID_DATA_DI_RTA
         |    where oiid is not null and trim(oiid)!='' and ieid is not null and trim(ieid)!=''
         |    group by oiid
         |    having count(1)>3
         |union
         |   select type, idvalue
         |    from $DW_GAASID_BLACKLIST
         |    where day='$full_par'
         |""".stripMargin
    )
  }

  def dw_gaasid_final_table(spark: SparkSession, insert_day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_GAASID_FINAL
         |select
         |distinct
         |if(ieid_black is null,ieid,'') as ieid,
         |if(oiid_black is null,oiid,'') as oiid
         |from
         |  (
         |  select a.ieid as ieid, a.oiid as oiid, b.ieid as ieid_black, c.oiid as oiid_black
         |  from $DW_GAAS_ID_DATA_DI_RTA a
         |      left join
         |        (
         |          select idvalue as ieid
         |          from $DW_GAASID_BLACKLIST
         |          where type='ieid' and day='$insert_day'
         |          group by idvalue
         |        ) b on a.ieid=b.ieid
         |      left join
         |        (
         |          select idvalue as oiid
         |          from $DW_GAASID_BLACKLIST
         |          where type='oiid' and day='$insert_day'
         |          group by idvalue
         |        ) c on a.oiid=c.oiid
         |  )d
         |""".stripMargin
    )
  }
}
