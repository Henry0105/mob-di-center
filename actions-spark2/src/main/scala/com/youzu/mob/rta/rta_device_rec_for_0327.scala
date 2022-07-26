package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object rta_device_rec_for_0327 {
  def dm_device_rec_for_0327_pre_table(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0327_PRE
         |    select '03271' as code,
         |        a.type as idtype,
         |        idvalue,
         |        0 as recommend,
         |        1 as status
         |    from $DW_GAASID_BLACKLIST a left anti
         |        join $DW_GAASID_DOUBTFUL b
         |      on a.type = b.type and a.idvalue = b.idvalue
         |union
         |    select '03272' as code,
         |        a.type as idtype,
         |        a.idvalue,
         |        0 as recommend,
         |        1 as status
         |    from $DW_GAASID_BLACKLIST a
         |        join $DW_GAASID_DOUBTFUL b
         |     on a.type = b.type and a.idvalue = b.idvalue
         |union
         |    select '03273' as code,
         |        a.type as idtype,
         |        a.idvalue,
         |        0 as recommend,
         |        1 as status
         |    from $DW_GAASID_DOUBTFUL a left anti
         |        join $DW_GAASID_BLACKLIST b
         |     on a.type = b.type and a.idvalue = b.idvalue
         |""".stripMargin
    )
  }

  def dm_device_rec_for_0327_table(spark: SparkSession, insert_day: String,full_par: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0327 partition(day='$insert_day')
         |   select
         |     coalesce(a.code, b.code) as code,
         |     coalesce(a.idtype, b.idtype) as idtype,
         |     coalesce(a.idvalue, b.idvalue) as idvalue,
         |     coalesce(a.recommend, b.recommend) as recommend,
         |     case when a.recommend = b.recommend then 2
         |          when a.recommend is not null
         |               and b.recommend is null then 1
         |          when a.recommend is not null
         |               and b.recommend is not null
         |               and a.recommend != b.recommend then 1
         |          else 0
         |     end as status
         |   from
         |     $DM_DEVICE_REC_FOR_0327_PRE a
         | full join
         |  (
         |    select code,idtype,idvalue,recommend
         |    from $DM_DEVICE_REC_FOR_0327
         |    where day = '$full_par'
         |  ) b
         |   on a.code = b.code and a.idtype = b.idtype and a.idvalue = b.idvalue
         |""".stripMargin
    )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("rta_device_rec_for_0327")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //传入日期参数
    val insert_day = args(0)
    val full_par = args(1)

    //新建临时表，包括本次所有0327推荐设备数据
    dm_device_rec_for_0327_pre_table(spark)
    println("step17: dm_device_rec_for_0327_pre_table 执行完毕")

    //更新至0327规则结果表最新分区，增加增改、删、不变的状态
    dm_device_rec_for_0327_table(spark,insert_day,full_par)
    println("step18: dm_device_rec_for_0327_table 执行完毕")

    //关闭程序
    spark.stop()
  }
}
