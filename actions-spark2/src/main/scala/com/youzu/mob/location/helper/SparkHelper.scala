package com.youzu.mob.location.helper

import com.youzu.mob.location.utils.DbscanUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @author zhoup
 *
 */
trait SparkHelper {


  val spark = SparkSession
    .builder()
    .enableHiveSupport()
    .config(getConfig())
    .getOrCreate()

  spark.udf.register("get_distance", DbscanUtils.distanceGPS)
  spark.udf.register("get_day_of_week", DbscanUtils.getDayForWeek)

  def getConfig(): SparkConf

}
