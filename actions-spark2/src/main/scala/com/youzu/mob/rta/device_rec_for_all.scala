package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object device_rec_for_all {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("device_rec_for_all")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //传入日期参数
    val insert_day = args(0)

    //汇总规则数据
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_ALL partition(day='$insert_day')
         |  select
         |    code,
         |    idtype,
         |    idvalue,
         |    recommend,
         |    status
         |    from $DM_DEVICE_REC_FOR_0324
         |    where day='$insert_day'
         |  union
         |    select
         |      code, idtype, idvalue, recommend, status
         |      from $DM_DEVICE_REC_FOR_0325
         |      where day='$insert_day'
         |  union
         |    select
         |      code, idtype, idvalue, recommend, status
         |      from $DM_DEVICE_REC_FOR_0326
         |      where day='$insert_day'
         |  union
         |    select
         |      code, idtype, idvalue, recommend, status
         |      from $DM_DEVICE_REC_FOR_0327
         |      where day='$insert_day'
         |  union
         |    select
         |      code, idtype, idvalue, recommend, status
         |      from $DM_DEVICE_REC_FOR_03281
         |      where day='$insert_day'
         |  union
         |    select
         |      code, idtype, idvalue, recommend, status
         |      from $DM_DEVICE_REC_FOR_03282
         |      where day='$insert_day'
         |""".stripMargin
    )

    spark.stop()
  }
}
