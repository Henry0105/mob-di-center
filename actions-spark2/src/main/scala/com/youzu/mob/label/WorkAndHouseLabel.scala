package youzu.mob.label

import org.apache.spark.sql.SparkSession

object WorkAndHouseLabel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    val workFrame = spark.sql(
      """
        |select device,lat,lon, begintime, day from
        |(select device,lat,lon, begintime, day
        |from test.all_data_work_home_tmp_xdzhang
        |where workperiod =1) a
        |left semi join
        |(select device from (
        | select device,Row_number()
        | over(partition by device order by device ) rk
        | from (
        | select device,Row_number() over(
        | partition by device order by device
        | ) rk from test.all_data_work_home_tmp_xdzhang
        | where workperiod =1
        | ) r where r.rk >=3) aa where aa.rk =1
        |) f on f.device = a.device
      """.stripMargin)

    println("-----all data---" + workFrame.count())
    val workRdd = workFrame.rdd.mapPartitions(rdds => {
      var res = List[(String, Iterable[(String, String)])]()
      while (rdds.hasNext) {
        val rdd = rdds.next()
        val device = rdd.getAs[String]("device")
        val lat = rdd.getAs[String]("lat")
        val lon = rdd.getAs[String]("lon")
        res.::=(device, Iterable((lat, lon)))
      }
      res.toIterator
    }).reduceByKey((x, y) => x ++ y)

    workRdd.mapPartitions(rdds => {
      var res = List[(String, Iterable[(String, String)])]()

      while (rdds.hasNext) {
        val rdd = rdds.next()
        val device = rdd._1
        val latlons = rdd._2.toArray


      }
      res.toIterator
    })
  }

}
