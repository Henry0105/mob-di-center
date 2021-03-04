package youzu.mob.label

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object LBS_Label_No_Extends {
  def main(args: Array[String]): Unit = {
    val sql = args(0)
    val insertsql = args(1)
    val filesnum = args(2).toInt
    val table_str = args(3)

    val spark = SparkSession.builder().enableHiveSupport()
      .appName(this.getClass.getSimpleName.stripSuffix("$")).getOrCreate()

    val dfs = spark.sql(sql)
    dfs.registerTempTable("data_tmp")
    spark.catalog.cacheTable("data_tmp")
    val groupRows
    = dfs.rdd.mapPartitions(rows => {
      var res = List[(String, Iterable[(String, String, String)])]()
      while (rows.hasNext) {
        val row = rows.next()
        val device = row.getAs[String]("device")
        val lat = row.getAs[String]("lat")
        val lon = row.getAs[String]("lon")
        val time = row.getAs[String]("time")
        res.::=(device, Iterable((lat, lon, time)))
      }
      res.toIterator
    }).reduceByKey((x, y) => x ++ y)
    import spark.implicits._
    val schemaString = "device,lat,lon,begintime,endtime"
    val re = groupRows.mapPartitions(d => {
      val listRdd = new ArrayBuffer[(String, String, String, String, String)]
      while (d.hasNext) {
        val maprows = d.next()
        val device = maprows._1
        val rows = maprows._2.toArray.sortWith((a, b) => {
          a._3 < b._3
        }).toIterator

        var old: (String, String, String) = null
        var now: (String, String, String) = null
        while (rows.hasNext) {
          val r = rows.next()
          val lat = r._1
          val lon = r._2
          if (null == old) {
            now = r
            old = r
          } else if (!old._1.equals(lat) || !old._2.equals(lon)) {
            listRdd += ((device, old._1, old._2, old._3, now._3))
            old = r
            now = r
          } else {
            now = r
          }
          if (!rows.hasNext) {
            listRdd += ((device, lat, lon, old._3, now._3))
          }
        }
      }
      listRdd.toIterator
    }).toDF(schemaString.split(","): _*)
    re.registerTempTable("lbs_tmp_1")
    spark.sql(
      s"""
         |cache table tmp_cache as select l.device,l.lat,
         |l.lon,l.begintime,l.endtime,${table_str} from lbs_tmp_1 l
         |join
         |data_tmp t
         |on t.device=l.device and l.begintime = t.time
      """.stripMargin)
    spark.sql(
      s"""
         |select device,lat,
         |lon,begintime,endtime,${table_str} from tmp_cache
      """.stripMargin).coalesce(filesnum).registerTempTable("tmp_info")
    spark.sql(insertsql)

  }

}
