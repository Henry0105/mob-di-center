package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object gaasid_doubtful {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("gaasid_doubtful")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val insert_day = args(0)
    val full_par = args(1)

    spark.sql(
      s"""
         |insert overwrite table $DW_GAASID_DOUBTFUL partition(day='$insert_day')
         |select
         |  'ieid' as type,
         |   ieid as idvalue
         |from $DWD_GAAS_RTA_ID_DATA_DI
         |  where day >= date_format(date_sub(current_date, 8), 'yyyyMMdd')
         |      and imei_md5_doubtful = '1'
         |  group by ieid
         |union
         |  select
         |    'oiid' as type,
         |     oiid as idvalue
         |  from $DWD_GAAS_RTA_ID_DATA_DI
         |  where day >= date_format(date_sub(current_date, 8), 'yyyyMMdd')
         |    and oaid_md5_doubtful = '1'
         |  group by oiid
         |union
         |  select
         |    type,idvalue
         |  from $DW_GAASID_DOUBTFUL
         |  where day ='$full_par'
         |""".stripMargin
    )

    spark.stop()
  }
}
