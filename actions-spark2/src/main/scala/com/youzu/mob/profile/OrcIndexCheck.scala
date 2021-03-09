package com.youzu.mob.profile


import com.youzu.mob.utils.Constants.INDEX_PROFILE_HISTORY_ALL
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}
import org.apache.hadoop.fs._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class OrcIndexCheck(
                           @transient spark: SparkSession,
                           table: String,
                           warehouse: String = "/user/hive/warehouse"
                         ) extends Logging {

  import spark.implicits._

  def check(day: String, prefix: String): Unit ={
    val Array(db, tableName) = table.split("\\.")
    val dir = s"$warehouse/$db.db/$tableName/day=$prefix$day"
    val conf = spark.sparkContext.hadoopConfiguration
    implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    val orcFile = spark.sparkContext.newAPIHadoopFile(
      dir,
      classOf[OrcIndexInputFormat],
      classOf[NullWritable],
      classOf[Text],
      conf
    )

    orcFile.map(_._2.toString).map { str =>
      //val Array(device, rowNumber, fileName) = str.split("\u0001")
      //(device, fileName, rowNumber.toInt)
      (str.split("\u0001")(0),str.split("\u0001")(2),str.split("\u0001")(1))

    }.toDF(
      "device", "file_name", "row_number"
    ).repartition(128).createOrReplaceTempView("t")

    spark.sql(
      s"""
         |select file_name,count(1) as cnt from t grouy by file_name
       """.stripMargin).createOrReplaceTempView("index_file")

    var start = day
    var end = day
    val filters = daysBetween(start, end)

    def cleanFn: UDF1[Map[String, Row], Map[String, Row]] = new UDF1[Map[String, Row], Map[String, Row]] {
      override def call(fi: Map[String, Row]): Map[String, Row] = {
        fi.filter(p => filters.contains(p._1))
      }
    }

    val schema = MapType(StringType, StructType(Array(
      StructField("file_name", StringType),
      StructField("row_number", LongType)
    )))

    spark.udf.register("clean_fn", cleanFn, schema)

    spark.sql(
      s"""
         |select file_name,count(1) as cnt from
         |(
         |select device,day,files.file_name as file_name from
         |(select device,clean_fn(feature_index) as feature_index from $INDEX_PROFILE_HISTORY_ALL where version='2020.1000')a
         | lateral view explode(feature_index) t as day,files
         |)a group by file_name
         |
       """.stripMargin).createOrReplaceTempView("all_index_file")

    spark.sql(
      s"""
         |insert overwrite table test.guanyt_check partition (day=$day)
         |select a.file_name,cnt from index_file a
         |join all_index_file b
         |on a.file_name=b.file_name
       """.stripMargin)



  }

  def daysBetween(start: String, end: String): Set[String] = {
    val s = DateTime.parse(start, DateTimeFormat.forPattern("yyyyMMdd"))
    val e = DateTime.parse(end, DateTimeFormat.forPattern("yyyyMMdd"))
    val days = Days.daysBetween(s, e).getDays
    (0 to days).map(s.plusDays).map(t => t.toString("yyyyMMdd")).toSet
  }






}

object OrcIndexCheck {
  def main(args: Array[String]): Unit = {
    val Array(start, end, inputTable, prefix) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index updater")
      .enableHiveSupport()
      .getOrCreate()

    val checker = new OrcIndexCheck(spark, inputTable)

    checker.daysBetween(start, end).toList.sorted.foreach { day =>
      println(s"day=>$prefix$day ...")
      checker.check(day, prefix)


      println(s"day=>$prefix$day finished\n\n\n")
    }

  }
}
