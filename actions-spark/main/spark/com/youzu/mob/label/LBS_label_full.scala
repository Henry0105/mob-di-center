package com.youzu.mob.label

import java.net.URL
import java.util

import com.mob.dm.lldistance.util.GeoHashUDF
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.mutable.ArrayBuffer

object LBS_label_full {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("Spark Application " + this.getClass.getSimpleName.stripSuffix("$"))
    val sc = new SparkContext(sparkConf)
    val hiveContext = new HiveContext(sc)
    URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory())
    hiveContext.sql("create temporary function coordConvertor as 'com.youzu.mob.java.udf.CoordConvertor'")
    val bsql1 = args(0)
    val bsql2 = args(1)
    val wsql1 = args(2)
    val wsql2 = args(3)
    val gsql1 = args(8)
    val gsql2 = args(9)
    val asql1 = args(10)
    val asql2 = args(11)
    val allsql = args(4)
    val insertsql = args(5)
    val partitions = args(6).toInt
    val filesnum = args(7).toInt

    hiveContext.sql(bsql1).registerTempTable("base_station_tmp1")
    hiveContext.sql(bsql2).registerTempTable("base_station_tmp2")
    hiveContext.sql(wsql1).registerTempTable("wifi_station_tmp1")
    hiveContext.sql(wsql2).registerTempTable("wifi_station_tmp2")
    hiveContext.sql(gsql1).registerTempTable("gps_station_tmp1")
    hiveContext.sql(gsql2).registerTempTable("gps_station_tmp2")
    hiveContext.sql(asql1).registerTempTable("autogps_station_tmp1")
    hiveContext.sql(asql2).registerTempTable("autogps_station_tmp2")

    val dfs = hiveContext.sql(allsql)
    val groupRows: RDD[(String, Iterable[
      (String, String, String, String, String, String, String, String, String, String, String, String, Double)])]
    = dfs.map(row => {
      val device = row.getAs[String]("device")
      val duid = row.getAs[String]("duid")
      val lat = row.getAs[String]("lat")
      val lon = row.getAs[String]("lon")
      val dtype = row.getAs[String]("type")
      val time = row.getAs[String]("time")
      val province = row.getAs[String]("province")
      val city = row.getAs[String]("city")
      val area = row.getAs[String]("area")
      val street = row.getAs[String]("street")
      val plat = row.getAs[String]("plat")
      val orig_note1 = row.getAs[String]("orig_note1")
      val orig_note2 = row.getAs[String]("orig_note2")
      val accuracy = row.getAs[Double]("accuracy")
      (device, (lat, lon, dtype, time, province, city, area, street, plat, duid, orig_note1, orig_note2, accuracy))
    }).groupByKey()
    import hiveContext.implicits._
    val schemaString = "device,duid,lat,lon,type,begintime,endtime,province,city," +
      "area,street,plat,orig_note1,orig_note2,accuracy"
    val re = groupRows.flatMap(maprows => {
      val device = maprows._1
      val rows = maprows._2.toArray.sortBy(_._4).toIterator

      val outlist = new ArrayBuffer[
        (String, String, String, String, String, String, String, String,
          String, String, String, String, String, String, Double)]
      var old: (String, String, String, String, String,
        String, String, String, String, String, String, String, Double) = null
      while (rows.hasNext) {
        val r = rows.next()
        val lat = r._1
        val lon = r._2
        if (null == old) {
          old = r
        } else if ((!old._1.equals(lat) || !old._2.equals(lon)) && timeDiff(old._4, r._4) >= 60) {
          outlist += (
            (device, old._10, old._1, old._2, old._3, old._4, r._4, old._5, old._6, old._7, old._8,
              old._9, old._11, old._12, old._13))
          old = r
        }
        if (!rows.hasNext) {
          outlist += ((device, r._10, lat, lon, r._3, old._4,
            "23:59:59", r._5, r._6, r._7, r._8, r._9, r._11, r._12, r._13))
        }

      }
      outlist
    }).toDF(schemaString.split(","): _*)

    re.registerTempTable("lbs_tmp_1")
    hiveContext.udf.register("getgeohash", GeoHashUDF.geoHashBase32ByLvUDF, StringType)
    hiveContext.sql(
      """
        |select device,duid,lat,lon,type,begintime,endtime,
        |province,city,area,street,plat,orig_note1,orig_note2,accuracy,
        |getgeohash(8,lat,lon) as geohash from lbs_tmp_1
      """.stripMargin).registerTempTable("tmp1")
    hiveContext.sql(
      """
        |cache table tmp_cache as select device,duid,lat,
        |lon,type,begintime,endtime,province,city,area,street,plat,orig_note1,orig_note2,accuracy,geohash from tmp1
      """.stripMargin)

    hiveContext.sql(
      """
        |select device,duid,lat,
        |lon,type,begintime,endtime,province,city,area,street,plat,orig_note1,orig_note2,accuracy,geohash from tmp_cache
      """.stripMargin).coalesce(filesnum).registerTempTable("tmp_info")

    hiveContext.sql(insertsql)
    sc.stop()
  }

  def timeDiff(beforeTime: String, afterTime: String): Long = {
    var time: Long = 0
    try {
      val fdf = FastDateFormat.getInstance("HH:mm:ss")
      val before = fdf.parse(beforeTime).getTime
      val after = fdf.parse(afterTime).getTime
      time = Math.abs(before - after) / 1000
    } catch {
      case ex: Exception => println(
        "java.text.ParseException: Unparseable date: beforeTime=>" + beforeTime + "|afterTime=>" + afterTime)
    }
    time
  }
}