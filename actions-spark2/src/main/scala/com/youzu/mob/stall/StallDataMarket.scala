package com.youzu.mob.stall

import org.apache.spark.sql.SparkSession

import com.youzu.mob.utils.Constants._
@deprecated
class StallDataMarket {

  /**
    * 设备在装applist
    *
    * @param spark
    * @param datetime
    */
  def applistIncr(spark: SparkSession, datetime: String): Unit = {

    spark.sql(
      s"""
         |insert overwrite table dm_mobdi_master.dm_device_applist_incr partition (day='${datetime}')
         |select device,concat_ws(',',sort_array(collect_set(pkg))) as applist,
         |       max(upload_time) as process_time
         |from
         |(
         |  select device,pkg,
         |         max(process_time) over (partition by device) as upload_time
         |  from $DWS_DEVICE_INSTALL_STATUS
         |  where day='${datetime}'
         |  and final_flag<>-1
         |) tt
         |where upload_time = '${datetime}'
         |group by device
      """.stripMargin)

  }

  def applistFull(spark: SparkSession, datetime: String, fulltime: String): Unit = {

    spark.sql(
      s"""
         |insert overwrite table dm_mobdi_master.dm_device_applist_full partition (day='${datetime}')
         |select nvl(a.device,b.device) as device,
         |       case when a.device is null then b.applist
         |            when b.device is null then a.applist
         |            when a.applist <> b.applist then b.applist
         |            else a.applist end as applist,
         |       case when b.device is not null then b.upload_time
         |            else a.process_time end as process_time,
         |       case when b.applist is not null and (a.applist is null or b.applist<>a.applist) then b.upload_time
         |            else a.process_time end as update_time
         |from
         |(
         |  select device,applist,process_time,update_time
         |  from dm_mobdi_master.dm_device_applist_full
         |  where day = '${fulltime}'
         |) a
         |full join
         |(
         |  select device,applist,upload_time
         |  from dm_mobdi_master.dm_device_applist_incr
         |  where day='${datetime}'
         |)b on a.device=b.device
      """.stripMargin)
  }


}

object StallDataMarket {

  def main(args: Array[String]): Unit = {
    val stallDataMarket = new StallDataMarket
    val datetime = args(0)
    val fulltime = args(1)
    val tableflag = args(2)
    val spark = initial(s"StallDataMarket ${tableflag}")

    tableflag match {
      case "applistIncr" => stallDataMarket.applistIncr(spark, datetime)
      case "applistFull" => stallDataMarket.applistFull(spark, datetime, fulltime)
    }

    spark.stop()
  }

}
