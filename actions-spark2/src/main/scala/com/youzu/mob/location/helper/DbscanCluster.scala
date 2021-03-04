package com.youzu.mob.location.helper

import com.youzu.mob.location.utils.DbscanUtils
import org.apache.spark.sql.{DataFrame, SparkSession}


object DbscanCluster {


  def cluster(spark: SparkSession, prepareSql: String,
              isNeedDeleteCache: Boolean, maximumErrorNumbers: Int, trainType: Int): DataFrame = {

    spark.sql(
      s"""
         |select device,lon,lat,count(1) over(partition by lon,lat) as err_num
         |from
         |(
         |  select device,lon,lat
         |  from
         |  (${prepareSql}) prepare
         |  group by device,lon,lat
         |)t
         |""".stripMargin)
      .createOrReplaceTempView("tmp_excep_data")
    spark.sql(
      s"""
         |select
         |   device,
         |   lon,
         |   lat,
         |   lon_new,
         |   lat_new,
         |   cast(get_distance(coalesce(lon,lon_new,0),
         |         coalesce(lat,lat_new,0),
         |         coalesce(lon_new,lon,0),
         |         coalesce(lat_new,lat,0)) as int) as distance_diff,
         |   datetime-datetime_new+0.1 as datetime_diff,
         |   time,
         |   datetime,
         |   weight,
         |   day
         |from
         |(
         |  select
         |     device,
         |     lon,
         |     lat,
         |     lag(lon,1) over(partition by device order by datetime) as lon_new,
         |     lag(lat,1) over(partition by device order by datetime) as lat_new,
         |     lag(datetime,1) over(partition by device order by datetime) as datetime_new,
         |     time,
         |     datetime,
         |     1 as weight,
         |     day
         |  from
         |  (
         |   ${prepareSql}
         |  )total
         |)t
       """.stripMargin)
      .createOrReplaceTempView("tmp_diff_neighbor")

    val filterData = if (isNeedDeleteCache) {
      spark.sql(
        s"""
           |select device,lat,lon,count(1) as err_num from
           |(
           |  select device,lat,lon
           |  from tmp_diff_neighbor
           |  where datetime_diff<=43200
           |  and distance_diff>=100
           |  and cast(nvl(distance_diff/datetime_diff,0) as bigint)>=30
           |  union all
           |  select device,lat_new as lat,lon_new as lon
           |  from tmp_diff_neighbor
           |  where datetime_diff<=43200
           |  and distance_diff>=100
           |  and cast(nvl(distance_diff/datetime_diff,0) as bigint)>=30
           |  and lat_new is not null and  lon_new is not null
           |)a
           |group by device,lat,lon
           |having err_num>=2
       """.stripMargin)
        .createOrReplaceTempView("tmp_err_num")

      spark.sql(
        s"""
           |select device,lon,lat
           |from
           |(
           |  select device,lon,lat
           |  from tmp_err_num
           |  where err_num>=2
           |  union all
           |  select device,lon,lat
           |  from tmp_excep_data
           |  where err_num>${maximumErrorNumbers}
           |)t
           |group by device,lon,lat
           |""".stripMargin)
    } else {
      spark.sql(s"select device,lon,lat from tmp_excep_data where err_num>${maximumErrorNumbers} ")
    }

    filterData.createOrReplaceTempView("tmp_filter_data")

    val clusterDataFrame = spark.sql(
      s"""
         |select * from
         |(
         |select tmp_diff_neighbor.device,tmp_diff_neighbor.lat,tmp_diff_neighbor.lon,hour(time) as hour,day,weight,
         |    row_number() over(partition by tmp_diff_neighbor.device order by rand()) rk
         |from
         |tmp_diff_neighbor
         |left  join
         |tmp_filter_data
         |on (tmp_diff_neighbor.device = tmp_filter_data.device and tmp_diff_neighbor.lon = tmp_filter_data.lon and tmp_diff_neighbor.lat = tmp_filter_data.lat)
         |where tmp_filter_data.device is  null
         |)t
         |where rk <=4000
       """.stripMargin)

    DbscanUtils.centerPoint(clusterDataFrame, spark, trainType)

  }


}
