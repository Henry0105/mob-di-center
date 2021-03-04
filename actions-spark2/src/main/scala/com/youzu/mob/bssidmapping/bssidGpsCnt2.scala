package com.youzu.mob.bssidmapping

import com.youzu.mob.tools.GPSDistance.{getCenter, getDistance}
import org.apache.spark.sql.SparkSession

object bssidGpsCnt2 {
  def main(args: Array[String]): Unit = {

    if (args.length != 4) {
      println(
        s"""
           |error number of input parameters,please check your input
           |parameters like:<data_pre_sql> <normal_out_put_table> <abnormal_out_put_table> <day>
         """.stripMargin)
      System.exit(-1)
    }
    println(args.mkString(","))

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    val sc = spark.sparkContext

    sc.setLogLevel("WARN")

    import spark.implicits._

    val data_pre_sql = args(0)
    val normal_out_put_table = args(1)
    val abnormal_out_put_table = args(2)
    val day = args(3)

    val bssidLatLonCnt2DF = spark.sql(data_pre_sql)

    bssidLatLonCnt2DF.createOrReplaceTempView("bssidlatloncnt2")

    // bssid, latlist, lonlist, acc_set
    val bssidLatLonCnt2DF1 = bssidLatLonCnt2DF.
      repartition(4000).map(
      bssidItem => {
        val bssid = bssidItem.getAs[String]("bssid")
        val latlist = bssidItem.getAs[Seq[Double]]("latlist")
        val lonlist = bssidItem.getAs[Seq[Double]]("lonlist")

        val distance = getDistance(lonlist(0), latlist(0), lonlist(1), latlist(1))
        if (distance <= 0.5 & distance >= 0) {
          val (latCenter, lonCenter) = getCenter(latlist, lonlist)
          (bssid, latCenter, lonCenter)
        } else ("", -999.toDouble, -999.toDouble)

      }).filter(_._1 != "").toDF("bssid", "lat", "lon")

    bssidLatLonCnt2DF1.createOrReplaceTempView("normal")

    spark.sql(
      s"""
         |insert overwrite table $normal_out_put_table partition(dt=$day)
         |select a.bssid, a.lat, a.lon, b.acc_set, b.ssid_set
         |from
         |(
         |  select bssid, lat, lon
         |  from normal
         |) as a
         |inner join
         |bssidlatloncnt2 as b on a.bssid = b.bssid
      """.stripMargin)

    spark.sql(
      s"""
         |insert overwrite table $abnormal_out_put_table partition(dt=$day)
         |select a.bssid, a.latlist, a.lonlist, a.acc_set, a.ssid_set
         |from bssidlatloncnt2 as a
         |left join
         |(
         |  select bssid
         |  from normal
         |) as b on a.bssid = b.bssid
         |where b.bssid is null
      """.stripMargin)
  }
}
