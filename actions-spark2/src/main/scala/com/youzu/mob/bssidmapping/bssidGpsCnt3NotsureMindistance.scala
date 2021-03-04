package com.youzu.mob.bssidmapping

import com.youzu.mob.mydbscan.{DbscanPonit, DbscanTools}
import com.youzu.mob.tools.GPSDistance.getDistance
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object bssidGpsCnt3NotsureMindistance {
  def main(args: Array[String]): Unit = {

    if (args.length != 6) {
      println(
        s"""
           |error number of input parameters,please check your input
           |parameters like:<cnt3_notsure_trans_pre_sql> <gps_cnt3_notsure_table>
           |<strangetable_gps_cnt3_table> <gps_cnt3_sure_pre_table> <gps_cnt3_pre_allinfo> <day>
         """.stripMargin)
      System.exit(-1)
    }
    println(args.mkString(","))

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val pre_sql = args(0)
    val gps_cnt3_notsure_table = args(1)
    val strangetable_gps_cnt3_table = args(2)
    val gps_cnt3_sure_pre_table = args(3)
    val gps_cnt3_pre_allinfo = args(4)
    val day = args(5)

    val testdata = spark.sql(pre_sql)
    val testdata_group = testdata.rdd.repartition(6000).map(rdds => {
      val bssid = rdds.getAs[String]("bssid")
      val latlist = rdds.getAs[Seq[Double]]("latlist")
      val lonlist = rdds.getAs[Seq[Double]]("lonlist")
      val cluster = rdds.getAs[Int]("cluster")
      (bssid, (cluster, lonlist, latlist))
    }).groupByKey()

    val testdata_summary = testdata_group.map(t => {
      val bssid = t._1
      val pointlist = t._2.toArray
      val cluster1 = pointlist(0)
      val cluster2 = pointlist(1)
      val lonlist1 = cluster1._2
      val latlist1 = cluster1._3
      val lonlist2 = cluster2._2
      val latlist2 = cluster2._3
      var distance = ArrayBuffer[Double]()

      for (i <- lonlist1.indices) {
        for (j <- lonlist2.indices) {
          val distance_tmp = getDistance(lonlist1(i), latlist1(i), lonlist2(j), latlist2(j))
          if (distance_tmp >= 0) {
            distance += distance_tmp
          }
        }
      }

      var mindistance = 0.0
      if (distance.isEmpty) {
        mindistance = 0 - 999.0
      } else {
        mindistance = distance.min
      }

      (bssid, distance, mindistance)
    }).toDF("bssid", "distance", "mindistance")

    testdata_summary.createOrReplaceTempView("mindistance_tmp")

    spark.sql(
      s"""
         |insert overwrite table $strangetable_gps_cnt3_table partition(dt=$day)
         |select bssid,
         |       collect_list(lat) as latlist,
         |       collect_list(lon) as lonlist,
         |       collect_list(concat_ws(',', acc_set)) as acc_set,
		     |       collect_set(concat_ws(',', ssid_set)) as ssid_set
         |from
         |(
         |  select a.bssid, a.lat, a.lon, a.acc_set, a.ssid_set
         |  from $gps_cnt3_pre_allinfo as a
         |  inner join
         |  (
         |    select bssid
         |    from  mindistance_tmp
         |    where mindistance > 0.5
         |    or mindistance = -999
         |    group by bssid
         |  ) as b on a.bssid = b.bssid
         |  where a.dt='$day'
         |) as d
         |group by bssid
      """.stripMargin)

    val testdata_centerpoint_pre = spark.sql(
      s"""
         |select a.bssid, a.lat, a.lon
         |from $gps_cnt3_notsure_table as a
         |inner join
         |(
         |  select bssid
         |  from mindistance_tmp
         |  where mindistance <= 0.5
         |  and mindistance >= 0
         |  group by bssid
         |) as b on a.bssid = b.bssid
         |where a.dt='$day'
      """.stripMargin)

    val points = testdata_centerpoint_pre.rdd.repartition(6000).map(rdd => {
      val point = new DbscanPonit
      val lat = rdd.getAs[Double]("lat")
      val lon = rdd.getAs[Double]("lon")
      val bssid = rdd.getAs[String]("bssid")
      (bssid, (lon, lat))
    }).groupByKey()

    val schemaString = "bssid,lon,lat,centerLon,centerLat"

    val re = points.flatMap(df => {
      val bssid = df._1
      val arr = df._2.toArray
      val points = arr.map(a => {
        val point = new DbscanPonit
        point.lat = a._2
        point.lon = a._1
        point
      })
      DbscanTools.centerPoint(points)
      points.map(a => (bssid, a.lon, a.lat, a.centerLon, a.centerLat))
    })
      .toDF(schemaString.split(","): _*)

    re.createOrReplaceTempView("data_tmp")

    spark.sql(
      s"""
         |insert overwrite table $gps_cnt3_sure_pre_table partition(dt=$day)
         |select bssid, lon, lat, centerLon, centerLat
         |from data_tmp
      """.stripMargin)
  }
}
