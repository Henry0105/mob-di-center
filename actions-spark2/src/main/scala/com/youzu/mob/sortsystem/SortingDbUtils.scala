package com.youzu.mob.sortsystem

import java.util.Properties

import com.youzu.mob.sink.{BuildUtils, MailSink2WithAttachments}
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks.{break, breakable}

object SortingDbUtils {

  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      println("ERROR: wrong number of parameters")
      println("USAGE: <mysqlInfoStr> <mailList> <insertTestTableSql> <checkDataSqls> <bakAndInsertSql> <doubleCheckSql>")
      System.exit(-1)
    }

    val mysqlInfoStr = args(0).trim
    val mailList = args(1).trim
    val insertTestTableSql = args(2).trim
    val checkDataSqls = args(3).trim
    val bakAndInsertSql = args(4).trim
    val doubleCheckSql = args(5).trim

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()

    etlData(spark, mysqlInfoStr)

    spark.sql(insertTestTableSql).createOrReplaceTempView("test_info_tmp")

    val sqls = checkDataSqls.split(";")

    var flag = false
    breakable {
      sqls.foreach(sql => {
        if (!checkData(spark, sql.trim, mailList)) {
          break()
        }
      })
      flag = true
    }

    println(
      s"""
         |check sql of $checkDataSqls result state is $flag
      """.stripMargin)

    if (flag) bakAndInsertData(spark, bakAndInsertSql)

    if (flag) {
      val doubleCheckSqls = doubleCheckSql.split(";")
      var doubleCheckFlag = false
      breakable {
        doubleCheckSqls.foreach(sql => {
          if (!checkData(spark, sql.trim, mailList)) {
            break()
          }
        })
        doubleCheckFlag = true
      }

      println(
        s"""
           |double check sql $doubleCheckSql result state is $doubleCheckFlag
      """.stripMargin)
    }

    spark.stop()
  }

  def etlData(spark: SparkSession, mysqlInfoStr: String): Unit = {
    val dbInfo = DbInfo.convertToDbInfo(mysqlInfoStr)
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", DbInfo.getJDBCUrl(dbInfo))
      .option("dbtable", DbInfo.getDbTable(dbInfo))
      .option("user", dbInfo.userName)
      .option("password", dbInfo.pwd)
      .option("driver", "com.mysql.jdbc.Driver")
      .load()

    jdbcDF.createOrReplaceTempView("mysql_table_info_tmp")
  }

  def checkData(spark: SparkSession, checkSql: String, mailList: String): Boolean = {
    val df = spark.sql(checkSql)
    val cnt = df.count()
    var result: Boolean = true
    val cot = df.collect()

    if (cnt != 0 && !cot.isEmpty) {
      result = false

      val contents = df.collect()

      val wb = BuildUtils.buildExcel(BuildUtils.buildRows(contents), "exception_results")

      val props: Properties = new Properties()
      props.setProperty("mail.smtp.auth", "true")
      props.setProperty("mail.host", "smtp-internal.mob.com")
      props.setProperty("mail.transport.protocol", "smtp")
      props.setProperty("mail.smtp.connectiontimeout", "120000")
      props.setProperty("mail.smtp.timeout", "120000")
      props.setProperty("mail.smtp.starttls.enable", "true")
      props.setProperty("mail.smtp.ssl.enable", "false")
      props.setProperty("mail.smtp.ssl.trust", "smtp-internal.mob.com")

      val mailSink2: MailSink2WithAttachments = new MailSink2WithAttachments(
        props,
        "bigdata_alert@mob.com",
        mailList.split(";"),
        "分拣系统mysql有不符合规范的数据被录入",
        s"错误数据请看附件，请周周及时处理<br>报错sql:<br>$checkSql ")
      mailSink2.init()
      mailSink2.attach(s"wrong_data.xlsx", wb)
      mailSink2.send()



    }
    result
  }

  def bakAndInsertData(spark: SparkSession, bakAndInsertSql: String): Unit = {
    val sqlArr = bakAndInsertSql.split(";")

    for (i <- 0 until sqlArr.length) {
      if (sqlArr(i).trim.length > 3) {
        spark.sql(sqlArr(i))
      }
    }
  }

}
