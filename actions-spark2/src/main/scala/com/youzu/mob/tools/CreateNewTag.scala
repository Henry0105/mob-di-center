package com.youzu.mob.tools

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.TimeWindow

object CreateNewTag {


  def main(args: Array[String]): Unit = {
    val sql1 = args(0)
    val sql2 = args(1)
    val sql3 = args(2)
    val f = new File("application.conf")
    if (!f.exists()) {
      throw new Exception(s"The configuration file does not exist!")
    }
    val conf = ConfigFactory.parseFile(f)

    val spark = SparkSession
      .builder()
      // .master("local[*]")
      .appName("create_new_tag")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.sql(sql1)

    val result = df.filter(x => {
      val feature = x.getString(1)
      val cnt = x.getString(2)
      val flag = x.getInt(3)
      val timewindow = x.getString(5)
      isTrue(conf, feature, cnt.toInt, flag, timewindow.toInt)
    })
    result.createOrReplaceTempView("timewindow_online_profile_tmp")

    val exectionDf = spark.sql(sql2)
    exectionDf.createOrReplaceTempView("evection_tag_tmp")
    spark.sql(sql3)
    spark.stop()
  }

  def isTrue(conf: Config, feature: String, cnt: Int, flag: Int, timewindow: Int): Boolean = {
    if (conf.hasPath(s"$feature.and")) {
      (conf.getInt(s"$feature.cnt") <= cnt.toInt &&
        conf.getInt(s"$feature.flag") == flag &&
        conf.getInt(s"$feature.timewindow") == timewindow.toInt) ||
        (conf.getInt(s"$feature.and.cnt") <= cnt.toInt &&
          conf.getInt(s"$feature.and.flag") == flag &&
          conf.getInt(s"$feature.and.timewindow") == timewindow.toInt)
    } else {
      conf.hasPath(s"$feature.cnt") &&
        conf.getInt(s"$feature.cnt") <= cnt.toInt &&
        conf.getInt(s"$feature.flag") == flag &&
        conf.getInt(s"$feature.timewindow") == timewindow.toInt
    }

  }

}
