package com.youzu.mob.location.workandlive

import com.youzu.mob.location.helper.{DbscanCluster, SparkHelper}
import com.youzu.mob.utils.Constants._
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel


object WorkAndLivePlaceClusterWithNothing extends SparkHelper {

  override def getConfig(): SparkConf = new SparkConf().setAppName("work live cluster")

  def main(args: Array[String]): Unit = {

    val stage = args(0)
    val prepareSQL = args(1)
    val trainType = args(2).toInt

    val generateSQLs = stage.substring(0, 1) match {
      case "A" =>
        preCluster(prepareSQL, stage, true, 300, trainType)
        val confidenceLive =
          s"""
             |case when liveplace = 3 then 0.85
             |     when live_day_flag=0 then 0.5
             |     when living_num1>=50 and live_days1>=10 then 1.0
             |     when living_num1>=10 and live_days1>=4 then 0.95
             |     else 0.90 end
           """.stripMargin
        val confidenceWork =
          s"""
             |case when workplace = 3 then 0.85
             |    when work_day_flag=0 then 0.5
             |    when work_num1>=50 and work_days1>=10 then 1.0
             |    when work_num1>=10 and work_days1>=4 then 0.95
             |    else 0.90 end
           """.stripMargin
        (generateConfidenceSQL(stage, confidenceLive, "", "live"),
          generateConfidenceSQL(stage, confidenceWork, "", "work"))

      case "B" =>
        preCluster(prepareSQL, stage, false, 3000, trainType)
        val confidenceLive =
          s"""
             |case when live_day_flag=0 and t1.device is not null then 0.5
             |     when live_day_flag=0 then 0.45
             |     when living_num1>=50 and live_days1>=10 then 0.8
             |     when living_num1>=10 and live_days1>=4 then 0.75
             |     else 0.70 end
           """.stripMargin
        val confidenceWork =
          s"""
             |case when work_day_flag=0 and t1.device is not null then 0.5
             |    when work_day_flag=0 then 0.45
             |    when work_num1>=50 and work_days1>=10 then 0.8
             |    when work_num1>=10 and work_days1>=4 then 0.75
             |    else 0.70 end
           """.stripMargin
        val joinLiveSql =
          s"""
             |left join
             |(
             |  select device from $DM_MOBDI_TMP.tmp_device_live_place where stage='A' group by device
             |)t1
             |on tmp_cluster_work_live.device  = t1.device
           """.stripMargin
        val joinWorkSql =
          s"""
             |left join
             |(
             |  select device from $DM_MOBDI_TMP.tmp_device_work_place where stage='A' group by device
             |)t1
             |on tmp_cluster_work_live.device = t1.device
           """.stripMargin
        (generateConfidenceSQL(stage, confidenceLive, joinLiveSql, "live"),
          generateConfidenceSQL(stage, confidenceWork, joinWorkSql, "work"))
      case "C" =>
        preCluster(prepareSQL, stage, false, 3000, trainType)
        val confidenceLive =
          s"""
             |  case when live_day_flag=0 and t1.device is not null then 0.5
             |     when live_day_flag=0 and t2.device is not null then 0.45
             |     when live_day_flag=0 then 0.10
             |     when living_num1>=50 and live_days1>=7 then 0.65
             |     when living_num1>=10 and live_days1>=5 then 0.60
             |     else 0.55 end
           """.stripMargin
        val confidenceWork =
          s"""
             |case when work_day_flag=0 and t1.device is not null then 0.5
             |    when work_day_flag=0 and t2.device is not null then 0.45
             |    when work_day_flag=0 then 0.10
             |    when work_num1>=50 and work_days1>=7 then 0.65
             |    when work_num1>=10 and work_days1>=5 then 0.60
             |    else 0.55 end
           """.stripMargin
        val joinLiveSql =
          s"""
             |left join
             |(
             |  select device from $DM_MOBDI_TMP.tmp_device_live_place where stage='A' group by device
             |)t1
             |on tmp_cluster_work_live.device  = t1.device
             |left join
             |(
             |  select device from $DM_MOBDI_TMP.tmp_device_live_place where stage='B' group by device
             |)t2
             |on tmp_cluster_work_live.device  = t2.device
           """.stripMargin
        val joinWorkSql =
          s"""
             |left join
             |(
             |  select device from $DM_MOBDI_TMP.tmp_device_work_place where stage='A' group by device
             |)t1
             |on tmp_cluster_work_live.device = t1.device
             |left join
             |(
             |  select device from $DM_MOBDI_TMP.tmp_device_work_place where stage='B' group by device
             |)t2
             |on tmp_cluster_work_live.device=t2.device
           """.stripMargin
        (generateConfidenceSQL(stage, confidenceLive, joinLiveSql, "live"),
          generateConfidenceSQL(stage, confidenceWork, joinWorkSql, "work"))
      case "D" =>
        spark.sql(
          s"""
             |select
             |  device,
             |  lon,
             |  lat,
             |  flag,
             |  total_num,
             |  active_days,
             |  row_number() over(partition by device,flag order by total_num desc) as rank
             |  from
             |  (
             |    select
             |       device,
             |       lon,
             |       lat,
             |       flag,
             |       max(active_days) as active_days,
             |       count(1) as total_num
             |    from
             |    (
             |    select
             |       device,
             |       lon,
             |       lat,
             |       flag,
             |       count(1) over(partition by device,flag,day) as active_days
             |    from
             |    (
             |      select device,lon,lat,day,
             |      case when hour>=8 and hour<=19 and get_day_of_week(day)>=1 and get_day_of_week(day)<=5 then 1
             |       when hour<=7 or hour>=20 or get_day_of_week(day) in (6,7) then 2
             |       else 0
             |       end as flag,
             |       day
             |      from
             |      (${prepareSQL})t
             |    )t1
             |    )t2
             |    group by device,lon,lat,flag
             |  )t3
       """.stripMargin)
          .createOrReplaceTempView("tmp_device_cluster_distribution")


        val work =
          s"""
             |insert overwrite  table $DM_MOBDI_TMP.tmp_device_work_place partition(stage='$stage')
             |select
             |   device,
             |   '' as cluster,
             |   lon as centerlon,
             |   lat as centerlat,
             |   total_num as work_num1,
             |   0 as living_num1,
             |   0 as distance_max,
             |   0 as distance_min,
             |  case when active_days <2 then 0.01
             |  else ln(total_num+0.1)/(2*(ln(total_num+0.1)+1))
             |  end as confidence
             |from
             |(
             |  select * from tmp_device_cluster_distribution where flag = 1 and rank = 1
             |)t
           """.stripMargin

        val live =
          s"""
             |insert overwrite  table $DM_MOBDI_TMP.tmp_device_live_place partition(stage='$stage')
             |select
             |   device,
             |   '' as cluster,
             |   lon as centerlon,
             |   lat as centerlat,
             |   0 as work_num1,
             |   total_num as living_num1,
             |   0 as distance_max,
             |   0 as distance_min,
             |  case when active_days <2 then 0.01
             |  else ln(total_num+0.1)/(2*(ln(total_num+0.1)+1))
             |  end as confidence
             |from
             |(
             |  select * from tmp_device_cluster_distribution where flag = 2 and rank = 1
             |)t
           """.stripMargin
        (live, work)
      case _ =>
        throw new IllegalArgumentException("No such table")
    }


    spark.sql(generateSQLs._1)
    spark.sql(generateSQLs._2)


  }


  private def preCluster(prepareSql: String, clusterTable: String,
                         isNeedDeleteCache: Boolean, maximumErrorNumbers: Int, trainType: Int): Unit = {

    DbscanCluster.cluster(spark, prepareSql, isNeedDeleteCache, maximumErrorNumbers, trainType)
      .createOrReplaceTempView("tmp_rs_dbscan")

    spark.sql(
      s"""
         |insert overwrite table $DM_MOBDI_TMP.tmp_device_dbscan_result partition(stage='$clusterTable')
         |select * from tmp_rs_dbscan
       """.stripMargin)
    spark.sql(
      s"""
         |select * from  $DM_MOBDI_TMP.tmp_device_dbscan_result where stage='$clusterTable'
       """.stripMargin).createOrReplaceTempView("tmp_dbscan_result")

    // 统计各个簇点数的情况。num2用来识别工作地与居住地在一起的情况
    // 查看每个簇占总数的比例情况（判断逻辑：工作时间去的概率最大的簇为工作地，居住地同理）
    spark.sql(
      s"""
         |select
         |   device,
         |   cluster,
         |   centerlon,
         |   centerlat,
         |   total_num,
         |   work_num1,
         |   living_num1,
         |   work_num2,
         |   living_num2,
         |   distance_max,
         |   distance_min,
         |   cluster_num,
         |   all_num,
         |   round(nvl(total_num/all_num*100,0),1) as rate_all,
         |   round(nvl(work_num1/all_work_num1*100,0),1) as rate_work_all,
         |   round(nvl(max_work_num1/all_work_num1*100,0),1) as rate_work_all_max,
         |   round(nvl(living_num1/all_living_num1*100,0),1) as rate_living_all,
         |   round(nvl(max_living_num1/all_living_num1*100,0),1) as rate_living_all_max
         |from
         |(
         |  select
         |     device,
         |     cluster,
         |     centerlon,
         |     centerlat,
         |     total_num,
         |     work_num1,
         |     living_num1,
         |     work_num2,
         |     living_num2,
         |     distance_max,
         |     distance_min,
         |     count(cluster) over(partition by device) as cluster_num,
         |     sum(total_num) over(partition by device) as all_num,
         |     sum(work_num1) over(partition by device) as all_work_num1,
         |     max(work_num1) over(partition by device) as max_work_num1,
         |     sum(living_num1) over(partition by device) as all_living_num1,
         |     max(living_num1) over(partition by device) as max_living_num1
         |  from
         |  (
         |    select
         |       device,
         |       cluster,
         |       centerlat,
         |       centerlon,
         |       sum(weight) as total_num,
         |       sum(case when hour>=8 and hour<=19 and weekday>=1 and weekday<=5 then weight else 0 end) as work_num1,
         |       sum(case when hour<=7 or hour>=20 or weekday in (6,7) then weight else 0 end) as living_num1,
         |       sum(case when hour in (13,14,15,16,17) and weekday>=1 and weekday<=5 then weight else 0 end) as work_num2,
         |       sum(case when hour in (23,0,1,2,3,4,5) then weight else 0 end) as living_num2,
         |       max(distance) distance_max,
         |       min(distance) distance_min
         |    from
         |    (
         |      select device,cluster,centerlon,centerlat,distance,day,hour,get_day_of_week(day) as weekday,weight
         |      from tmp_dbscan_result
         |    )t1
         |    where cluster!=0 and cluster is not null
         |    group by device,cluster,centerlat,centerlon
         |  )t2
         |)t3
       """.stripMargin)
      .createOrReplaceTempView("tmp_device_cluster_distribution")


    spark.sql(
      s"""
         |select device,cluster,date_flag,count(1) as cnt
         |from
         |(
         |  select
         |     device,
         |     cluster,
         |     date_flag
         |  from
         |  (
         |    select
         |       device,
         |       cluster,
         |       day,
         |       hour,
         |       weekday,
         |       case when  weekday between 1 and 5 and hour in (13,14,15,16,17) then 1
         |         when weekday between 1 and 5 and hour between 8 and 19 then 2
         |         when hour in (23,0,1,2,3,4,5) then 3
         |         else 4
         |       end as date_flag
         |    from
         |    (
         |      select device,cluster,day,hour,get_day_of_week(day) as weekday
         |      from tmp_dbscan_result
         |    )t1
         |    where cluster!=0 and cluster is not null
         |    group by device,cluster,day,hour,weekday
         |  )t2
         |  group by device,cluster,day,date_flag
         |)t3
         |group by device,cluster,date_flag
       """.stripMargin)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)
      .createOrReplaceTempView("tmp_device_cluster_count")

    spark.sql(
      s"""
         |insert overwrite table $DM_MOBDI_TMP.tmp_device_location_cluster_rank partition(stage='$clusterTable')
         |select
         |   device,
         |   cluster,
         |   centerlon,
         |   centerlat,
         |   total_num,
         |   work_num1,
         |   living_num1,
         |   work_num2,
         |   living_num2,
         |   cluster_num,
         |   all_num,
         |   rate_all,
         |   rate_work_all,
         |   rate_work_all_max,
         |   rate_living_all,
         |   rate_living_all_max,
         |   total_days,
         |   work_days1,
         |   work_day_flag,
         |   live_days1,
         |   live_day_flag,
         |   work_days2,
         |   live_days2,
         |   distance_max,
         |   distance_min,
         |   row_number() over(partition by device order by work_day_flag desc,work_num1 desc,work_days1 desc) as work_rank,
         |   row_number() over(partition by device order by live_day_flag desc,living_num1 desc,live_days1 desc) as live_rank
         |from
         |(
         |  select
         |     tmp_device_cluster_distribution.device,
         |     tmp_device_cluster_distribution.cluster,
         |     centerlon,
         |     centerlat,
         |     total_num,
         |     work_num1,
         |     living_num1,
         |     work_num2,
         |     living_num2,
         |     cluster_num,
         |     all_num,
         |     rate_all,
         |     rate_work_all,
         |     rate_work_all_max,
         |     rate_living_all,
         |     rate_living_all_max,
         |     distance_max,
         |     distance_min,
         |     nvl(total_day.cnt,0) as total_days,
         |     nvl(work_count.cnt,0) as work_days1,
         |     if(work_count.cnt>=2,1,0) as work_day_flag,
         |     nvl(live_count.cnt,0) as live_days1,
         |     if(live_count.cnt>2,1,0) as live_day_flag,
         |     nvl(work_serial_count.cnt,0) as work_days2,
         |     nvl(live_serial_count.cnt,0) as live_days2
         |  from
         |  tmp_device_cluster_distribution
         |  left join
         |  (
         |     select device,cluster,sum(cnt) as cnt from tmp_device_cluster_count where date_flag in(1,2) group by device,cluster
         |  )work_count
         |  on tmp_device_cluster_distribution.device = work_count.device and tmp_device_cluster_distribution.cluster = work_count.cluster
         |  left join
         |  (
         |     select device,cluster,sum(cnt) as cnt from tmp_device_cluster_count where date_flag in(3,4) group by device,cluster
         |  )live_count
         |  on tmp_device_cluster_distribution.device = live_count.device and tmp_device_cluster_distribution.cluster = live_count.cluster
         |  left join
         |  (
         |     select device,cluster,cnt from tmp_device_cluster_count where date_flag = 1
         |  )work_serial_count
         |  on tmp_device_cluster_distribution.device = work_serial_count.device and tmp_device_cluster_distribution.cluster = work_serial_count.cluster
         |  left join
         |  (
         |     select device,cluster,cnt from tmp_device_cluster_count where date_flag = 3
         |  )live_serial_count
         |  on tmp_device_cluster_distribution.device = live_serial_count.device and tmp_device_cluster_distribution.cluster = live_serial_count.cluster
         |  left join
         |  (
         |     select device,cluster,sum(cnt) as cnt from tmp_device_cluster_count group by device,cluster
         |  )total_day
         |  on tmp_device_cluster_distribution.device = total_day.device and tmp_device_cluster_distribution.cluster = total_day.cluster
         |)t
       """.stripMargin)
    spark.sql(
      s"""
         |select
         |   device,
         |   cluster,
         |   centerlon,
         |   centerlat,
         |   total_num,
         |   work_num1,
         |   living_num1,
         |   work_num2,
         |   living_num2,
         |   cluster_num,
         |   all_num,
         |   rate_all,
         |   rate_work_all,
         |   rate_work_all_max,
         |   rate_living_all,
         |   rate_living_all_max,
         |   total_days,
         |   work_days1,
         |   work_day_flag,
         |   live_days1,
         |   live_day_flag,
         |   work_days2,
         |   live_days2,
         |   work_rank,
         |   live_rank,
         |   distance_max,
         |   distance_min,
         |   case
         |     when workplace <>2 then workplace
         |     when (work_rank = 1 and work_cluster_cnt =1 and live_cluster_cnt>=2) or (work_rank = 2 and work_cluster_cnt >2 and live_cluster_cnt = 1) then 1
         |     WHEN (work_cluster_cnt >=2 and live_cluster_cnt >=2
         |       and ((rate_work_all_max>rate_living_all_max and work_rank = 1 ) or (rate_work_all_max<=rate_living_all_max and work_rank =2)))
         |     then 1
         |     when work_cluster_cnt = 1 and live_cluster_cnt = 1 then 3
         |     else workplace
         |     end as workplace,
         |   case
         |     when liveplace <> 2 then liveplace
         |     when (live_rank = 2 and work_cluster_cnt =1 and live_cluster_cnt>=2) or (live_rank = 1 and work_cluster_cnt >2 and live_cluster_cnt = 1) then 1
         |     when  (work_cluster_cnt >=2 and live_cluster_cnt >=2
         |       and ((rate_work_all_max>rate_living_all_max and live_rank = 2 ) or (rate_work_all_max<=rate_living_all_max and live_rank =1)))
         |      then 1
         |     when work_cluster_cnt = 1 and live_cluster_cnt = 1 then 3
         |     else workplace
         |     end as liveplace
         |from
         |(
         |  select
         |     device,
         |     cluster,
         |     centerlon,
         |     centerlat,
         |     total_num,
         |     work_num1,
         |     living_num1,
         |     work_num2,
         |     living_num2,
         |     cluster_num,
         |     all_num,
         |     rate_all,
         |     rate_work_all,
         |     rate_work_all_max,
         |     rate_living_all,
         |     rate_living_all_max,
         |     total_days,
         |     work_days1,
         |     work_day_flag,
         |     live_days1,
         |     live_day_flag,
         |     work_days2,
         |     live_days2,
         |     work_rank,
         |     live_rank,
         |     workplace,
         |     liveplace,
         |     distance_max,
         |     distance_min,
         |     sum(case when workplace =2 then 1 else 0 end ) over(partition by device) as work_cluster_cnt,
         |     sum(case when liveplace =2 then 1 else 0 end ) over(partition by device) as live_cluster_cnt
         |  from
         |  (
         |      select
         |      device,
         |      cluster,
         |      centerlon,
         |      centerlat,
         |      total_num,
         |      work_num1,
         |      living_num1,
         |      work_num2,
         |      living_num2,
         |      cluster_num,
         |      all_num,
         |      rate_all,
         |      rate_work_all,
         |      rate_work_all_max,
         |      rate_living_all,
         |      rate_living_all_max,
         |      total_days,
         |      work_days1,
         |      work_day_flag,
         |      live_days1,
         |      live_day_flag,
         |      work_days2,
         |      live_days2,
         |      work_rank,
         |      live_rank,
         |      distance_max,
         |      distance_min,
         |      case
         |        when (work_rank = 1 and work_num1>3 and cnt = 2) or (cnt = 1 and work_days2>=3 and live_days2>=3 and  work_days1>=2 and live_days1>=2)
         |          or (cnt = 1 and (work_days1 <2 or live_days1 <2) and work_num1>=3 and work_rank =1) then 1
         |        when cnt = 1 and  (work_days2<3 or live_days2<3) and work_days1>=2 and live_days1>=2 and work_num1>=3 and living_num1>=3
         |          and (rate_work_all_max-rate_work_all)<=0.1 and rate_work_all>0.1 and work_day_flag=1 and work_num1>=3 then 2
         |        else 0
         |        end as workplace,
         |      case
         |        when (live_rank = 1 and living_num1>3 and cnt = 2) or ( cnt = 1 and work_days2>=3 and live_days2>=3 and  work_days1>=2 and live_days1>=2)
         |           or (cnt = 1 and (work_days1 <2 or live_days1 <2) and living_num1>=3 and live_rank =1) then 1
         |        when cnt = 1 and (work_days2<3 or live_days2<3) and work_days1>=2 and live_days1>=2 and work_num1>=3 and living_num1>=3
         |          and (rate_living_all_max-rate_living_all)<=0.1 and rate_living_all>0.1 and live_day_flag=1 and living_num1>=3 then 2
         |        else 0
         |        end as liveplace
         |      from
         |      (
         |        select
         |           device,
         |           cluster,
         |           centerlon,
         |           centerlat,
         |           total_num,
         |           work_num1,
         |           living_num1,
         |           work_num2,
         |           living_num2,
         |           cluster_num,
         |           all_num,
         |           rate_all,
         |           rate_work_all,
         |           rate_work_all_max,
         |           rate_living_all,
         |           rate_living_all_max,
         |           total_days,
         |           work_days1,
         |           work_day_flag,
         |           live_days1,
         |           live_day_flag,
         |           work_days2,
         |           live_days2,
         |           work_rank,
         |           live_rank,
         |           distance_max,
         |           distance_min,
         |           count(1) over(partition by device) as cnt
         |        from
         |        $DM_MOBDI_TMP.tmp_device_location_cluster_rank
         |        where (work_rank=1 or live_rank=1) and stage ='$clusterTable'
         |      )t1
         |  )t2
         |)t3
       """.stripMargin)
      .createOrReplaceTempView("tmp_cluster_work_live")
  }

  private def generateConfidenceSQL(clusterTable: String, confidence: String, joinSql: String, name: String) = {

    s"""
       |insert overwrite table $DM_MOBDI_TMP.tmp_device_${name}_place partition(stage='$clusterTable')
       |select
       |   tmp_cluster_work_live.device,
       |   cluster,
       |   centerlon,
       |   centerlat,
       |   work_num1,
       |   living_num1,
       |   distance_max,
       |   distance_min,
       |   ${confidence} as confidence
       |from tmp_cluster_work_live
       |${joinSql}
       | where ${name}place in (1,3)
       """.stripMargin
  }

}
