package com.youzu.mob.mydbscan

import java.util.Date
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

object DBSCANTest_time {

  def main(args: Array[String]) {

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val parsedData = spark.sql("select lat, lon, bssid from test.zhangxy_mapping_bssid_orginal_cnt3_sample")
    val points = parsedData.rdd.repartition(4001).mapPartitions(rdds => {
      val retn = ArrayBuffer[(String, Iterable[DbscanPonit])]()
      while (rdds.hasNext) {
        val rdd = rdds.next()
        val point = new DbscanPonit
        point.lon = rdd.getAs[Double]("lon")
        point.lat = rdd.getAs[Double]("lat")
        retn += ((rdd.getAs[String]("bssid"), Iterable(point)))
      }
      retn.toIterator
    }).reduceByKey((x, y) => x ++ y)

    val schemaString = "bssid,lon,lat,cluster"
    import spark.implicits._
    val rp = points.repartition(4002).mapPartitions(rdds => {
      val reList = ArrayBuffer[(String, Double, Double, Int)]()
      while (rdds.hasNext) {
        val rdd = rdds.next()
        val bssid = rdd._1
        val points = rdd._2.toArray
        val points_length = points.length
        val cnt = 0
        val minPoints = cnt match {
          case cnt if (points_length < 100) => 3
          case cnt if (points_length >= 100 && points_length < 1000) => 5
          case cnt if (points_length >= 1000 && points_length < 5000) => 50
          case cnt if (points_length >= 5000 && points_length < 10000) => 200
          case cnt if (points_length >= 10000) => 500
        }
        Dbscan.train(points, 0.1, minPoints, true)

        reList ++= points.map(a => (bssid, a.lon, a.lat, a.cluster))
      }
      reList.toIterator
    }).toDF(schemaString.split(","): _*)

    rp.registerTempTable("data_tmp")
    spark.sql("drop table if exists test.dbscan_test_return_xdzhang_time")
    spark.sql(
      """
        |create table if not exists test.dbscan_test_return_xdzhang_time as
        |select bssid,lon, lat, cluster from data_tmp
      """.stripMargin)
  }
}
