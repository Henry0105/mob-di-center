package com.youzu.mob.sortsystem

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object SortSystemCheck {

  def main(args: Array[String]): Unit = {

    if (args.length != 6) {
      println("ERROR: wrong number of parameters")
      println("USAGE: <mysqlInfo_category> <mysqlInfo_tmp> <selectSql> <hive_db> <hive_table> <day>")
      System.exit(-1)
    }

    val mysqlInfo_category = args(0).trim
    val mysqlInfo_tmp = args(1).trim
    val selectSql = args(2).trim
    val hive_db = args(3).trim
    val hive_table = args(4).trim
    val day = args(5).trim

    val spark = SparkSession.builder().appName("SortSystemCheck").enableHiveSupport().getOrCreate()
    getDataFromMysql(spark, mysqlInfo_category).createOrReplaceTempView("app_category")

    val result: DataFrame = spark.sql(selectSql).cache()
    result.createOrReplaceTempView("result_tmp")

    // 数据进入hive表
    spark.sql(
      s"""
        |insert overwrite table ${hive_db}.${hive_table} partition (day='${day}')
        |select * from result_tmp
        |""".stripMargin)

    // 写数据到mysql中
    writeDataToMysql(result, spark, mysqlInfo_tmp)

    spark.stop()
  }

  /**
   * 从mysql中读取数据
   * @param spark
   * @param mysqlInfoStr
   * @return
   */
  def getDataFromMysql(spark: SparkSession, mysqlInfoStr: String): DataFrame = {
    val dbInfo = DbInfo.convertToDbInfo(mysqlInfoStr)
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", DbInfo.getJDBCUrl(dbInfo))
      .option("dbtable", DbInfo.getDbTable(dbInfo))
      .option("user", dbInfo.userName)
      .option("password", dbInfo.pwd)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()
    jdbcDF
  }

  /**
   * 数据写入mysql (append)
   * @param data
   * @param spark
   * @param mysqlInfoStr
   */
  def writeDataToMysql(data : DataFrame, spark: SparkSession, mysqlInfoStr: String): Unit = {
    val dbInfo = DbInfo.convertToDbInfo(mysqlInfoStr)
    data.write.format("jdbc")
      .option("url", DbInfo.getJDBCUrl(dbInfo))
      .option("dbtable", DbInfo.getDbTable(dbInfo))
      .option("user", dbInfo.userName)
      .option("password", dbInfo.pwd)
      .option("driver", "com.mysql.jdbc.Driver")
      .mode(SaveMode.Overwrite)
      .save()
  }

}
