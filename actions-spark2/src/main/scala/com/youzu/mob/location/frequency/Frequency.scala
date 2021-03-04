package com.youzu.mob.location.frequency

import com.youzu.mob.location.helper.{DbscanCluster, SparkHelper}
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel

object Frequency extends SparkHelper {


  override def getConfig(): SparkConf = new SparkConf()
    .setAppName("Frequency Place")

  def main(args: Array[String]): Unit = {

    val clusterTable = args(0)
    val prepareSql = args(1)
    val trainType = args(2).toInt


    clusterTable match {
      case "A" =>
        useTableContainsA(clusterTable)
      case "D" =>
        finalSummary(prepareSql, clusterTable)
      case _ =>
        useTableNotContainsA(prepareSql, clusterTable, trainType)
    }

  }


  private def useTableNotContainsA(prepareSql: String, clusterTable: String, trainType: Int): Unit = {

    DbscanCluster.cluster(spark, prepareSql, false, 3000, trainType)
      .createOrReplaceTempView("tmp_dbscan_result")
    finalSummary("select * from tmp_dbscan_result", clusterTable)
  }


  private def finalSummary(prepareSql: String, clusterTable: String): Unit = {

    spark.sql(
      s"""
         |select device,cluster,centerlon,centerlat,cnt_active,total_cluster_cnt
         |from
         |(
         |  select device,cluster,centerlon,centerlat,count(1) as cnt_active,sum(cnt) as total_cluster_cnt
         |  from
         |  (
         |    select device,cluster,centerlon,centerlat,day,count(1) as cnt
         |    from (${prepareSql}) prepare
         |    where cluster!=0 and cluster is not null
         |    group by device,cluster,centerlon,centerlat,day
         |  )t1
         |  group by device,centerlon,centerlat,cluster
         |)t2
         |where cnt_active > 2 and total_cluster_cnt >3
         |
       """.stripMargin)
      .createOrReplaceTempView("tmp_result_distribution")


    spark.sql(
      s"""
         |insert overwrite table dw_mobdi_md.tmp_device_frequency_place partition(stage='$clusterTable')
         |select
         |   device,
         |   cluster,
         |   centerlon,
         |   centerlat,
         |   ${getConfidenceSql(clusterTable)} as confidence,
         |   cnt_active
         |from
         |(
         |  select
         |     tmp_result_distribution.device,
         |     cluster,
         |     centerlon,
         |     centerlat,
         |     cnt_active,
         |     total_cluster_cnt,
         |     case when b.device is null then 210
         |       else get_distance(centerlon,centerlat,lon,lat)
         |       end as distance
         |   from
         |  tmp_result_distribution
         |  left join
         |  (
         |    select device,lon,lat
         |    from
         |    (
         |      select device,centerlon as lon ,centerlat as lat
         |      from dw_mobdi_md.tmp_device_work_place
         |      where stage='$clusterTable'
         |      union all
         |      select device,centerlon as lon ,centerlat as lat
         |      from dw_mobdi_md.tmp_device_live_place
         |      where stage='$clusterTable'
         |      union all
         |      select device,centerlon as lon ,centerlat as lat
         |      from dw_mobdi_md.tmp_device_frequency_place
         |    )t
         |    group by device,lon,lat
         |  )b
         |  on tmp_result_distribution.device = b.device
         |)tt
         |where distance>200
         |group by device,cluster,centerlon,centerlat,cnt_active,total_cluster_cnt
       """.stripMargin)
  }


  private def getConfidenceSql(clusterTable: String): String = {
    clusterTable match {
      case "B" =>
        s"""
           |   case
           |     when total_cluster_cnt>=50 and cnt_active>7 then 0.8
           |     when total_cluster_cnt>=10 and cnt_active>5 then 0.75
           |     else 0.7
           |     end
         """.stripMargin
      case "C" =>
        s"""
           | case
           |     when total_cluster_cnt>=50 and cnt_active>7 then 0.65
           |     when total_cluster_cnt>=10 and cnt_active>5 then 0.60
           |     else 0.55
           |     end
         """.stripMargin
      case "D" =>
        "ln(total_cluster_cnt+0.1)/(2*(ln(total_cluster_cnt+0.1)+1))"
      case _ =>
        throw new IllegalStateException("No such table")
    }
  }

  private def useTableContainsA(clusterTable: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table dw_mobdi_md.tmp_device_frequency_place partition(stage='$clusterTable')
         |select a.device,a.cluster,centerlon,centerlat,
         |case when total_num>=50 and total_days>=7 then 1
         |     when total_num>=10 and total_days>=5 then 0.95
         |     else 0.9 end as confidence,
         |total_days as cnt_active
         |from
         |(
         |select
         |   device,
         |   cluster,
         |   centerlon,
         |   centerlat,
         |   total_num,
         |   total_days
         |   from dw_mobdi_md.tmp_device_location_cluster_rank
         |   where stage='$clusterTable' and total_days>=2
         |)a
         |left join
         |(
         |    select device,cluster
         |    from dw_mobdi_md.tmp_device_live_place
         |    where stage = '$clusterTable'
         |    union all
         |    select device,cluster
         |    from dw_mobdi_md.tmp_device_work_place
         |    where stage ='$clusterTable'
         |)b
         |on a.device = b.device and a.cluster = b.cluster
         |where b.device is null
       """.stripMargin)
  }


}
