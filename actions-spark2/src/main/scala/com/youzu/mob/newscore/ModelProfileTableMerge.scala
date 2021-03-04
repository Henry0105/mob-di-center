package com.youzu.mob.newscore

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object ModelProfileTableMerge {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("ModelProfileTableMerge")
    val spark = SparkSession
      .builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    // 1 read FROM config file
    if (args.length != 2 || args(0).isEmpty || args(1).isEmpty) {
      throw new IllegalArgumentException("Parameter number error!")
    }
    val insertDay = args(0) // 本次插入周期
    val configFile = args(1) // 配置文件

    ConfigurationManager.init(configFile)
    val tagSeq = ConfigurationManager.getTagSeq
    ConfigurationManager.checkData(tagSeq)

    val targetTable = ConfigurationManager.getString("table.target.name")
    // 2 read profile table and update files
    val allCols = spark.sql(s"SELECT * FROM $targetTable LIMIT 1").columns.toSeq.diff(Seq("device", "day"))

    var colInfoMap: Map[String, Int] = Map()
    var colIndex = 1
    for (col <- allCols) {
      colInfoMap += (col -> colIndex)
      colIndex = colIndex + 1
    }

    println(colInfoMap)

    // 3 mergefile with union all
    val selectColSql = StringBuilder.newBuilder
    for (tagName <- tagSeq) {
      readData2TempViewTable(spark, tagName, insertDay)
      selectColSql.append("SELECT device, ")
        .append(formatSelectCol(colInfoMap, tagName))
        .append(" FROM TEMPVIEW_TABLE_" + tagName)
        .append(" UNION ALL \n")
    }

    //  将每日新增的模型数据union整合并根据device去重
    val unionSql =
      s"""
         |SELECT
         |device, ${formatMAXCol(allCols)}
         |FROM (
         |${selectColSql.substring(0, selectColSql.length - 12)}
         |) t
         |GROUP BY t.device
       """.stripMargin

    println(unionSql)
    val unionDF = spark.sql(unionSql)
    unionDF.createOrReplaceTempView("UNIONED_TABLE")

    // 根据revise规则重新计算需要revise的字段[r1阶段，r2阶段，r3阶段]
    val reviseMap = ConfigurationManager.getReviseMap

    val reviseR1eq = ConfigurationManager.getString("reviseR1").split(",").map(str => str.trim).toSeq

    val reviseR1JoinSql = StringBuilder.newBuilder
    for (reviseCol <- reviseR1eq) {
      if (reviseMap.contains("reviseR1." + reviseCol)) {
        reviseR1JoinSql.append(
          s"""
             |LEFT JOIN TEMPVIEW_TABLE_${reviseCol} ${reviseCol} ON (${reviseCol}.device=UNIONED_TABLE.device)\n
           """.stripMargin)
      }
    }

    val revisedR1Sql =
      s"""
         |SELECT
         |UNIONED_TABLE.device,
         |${formatReviseCol("UNIONED_TABLE", allCols, reviseR1eq, reviseMap, "reviseR1")}
         |FROM UNIONED_TABLE
         |${reviseR1JoinSql.toString()}
       """.stripMargin
    println(revisedR1Sql)
    spark.sql(revisedR1Sql).createOrReplaceTempView("REVISED_V1_TABLE")

    // reviseR2
    val reviseR2Seq = ConfigurationManager.getString("reviseR2").split(",").map(str => str.trim).toSeq

    val reviseR2JoinSql = StringBuilder.newBuilder
    for (reviseCol <- reviseR2Seq) {
      if (reviseMap.contains("reviseR2." + reviseCol)) {
        reviseR2JoinSql.append(
          s"""
             |LEFT JOIN TEMPVIEW_TABLE_${reviseCol} ${reviseCol} ON (${reviseCol}.device=REVISED_V1_TABLE.device)\n
           """.stripMargin)
      }
    }

    val revisedR2Sql =
      s"""
         |SELECT
         |REVISED_V1_TABLE.device,
         |${formatReviseCol("REVISED_V1_TABLE", allCols, reviseR2Seq, reviseMap, "reviseR2")}
         |FROM REVISED_V1_TABLE
         |${reviseR2JoinSql.toString()}
       """.stripMargin
    println(revisedR2Sql)
    spark.sql(revisedR2Sql).createOrReplaceTempView("REVISED_V2_TABLE")

    // reviseR3
    val reviseR3Seq = ConfigurationManager.getString("reviseR3").split(",").map(str => str.trim).toSeq

    val reviseR3JoinSql = StringBuilder.newBuilder
    for (reviseCol <- reviseR3Seq) {
      if (reviseMap.contains("reviseR3." + reviseCol)) {
        reviseR3JoinSql.append(
          s"""
             |LEFT JOIN TEMPVIEW_TABLE_${reviseCol} ${reviseCol} ON (${reviseCol}.device=REVISED_V2_TABLE.device)\n
           """.stripMargin)
      }
    }

    val revisedR3Sql =
      s"""
         |INSERT OVERWRITE TABLE $targetTable partition(day='$insertDay')
         |SELECT
         |REVISED_V2_TABLE.device,
         |${formatReviseCol("REVISED_V2_TABLE", allCols, reviseR3Seq, reviseMap, "reviseR3")}
         |FROM REVISED_V2_TABLE
         |${reviseR3JoinSql.toString()}
       """.stripMargin
    println(revisedR3Sql)
    spark.sql(revisedR3Sql)
    spark.stop()
  }

  // 用于添加revise字段的内容
  def formatReviseCol(tableName: String, allCols: Seq[String], reviseCols: Seq[String],
    reviseMap: Map[String, String], reviseLevel: String): String = {
    val sb = StringBuilder.newBuilder
    for (col <- allCols) {
      if (reviseCols.contains(col) && reviseMap.contains(reviseLevel + "." + col)) {
        val rule = reviseMap.get(reviseLevel + "." + col)
        sb.append(rule.get).append(" AS ").append(col).append(", \n")
      } else {
        sb.append(tableName + "." + col).append(" AS ").append(col).append(", \n")
      }
    }
    sb.substring(0, sb.length - 3)
  }

  def formatMAXCol(targetTableCols: Seq[String]): String = {
    val sb = new ArrayBuffer[String]()

    for (col <- targetTableCols) {
      if (col.endsWith("_cl")) {
        sb.append("round(MAX(" + col + "),4) AS " + col)
      } else {
        sb.append("MAX(" + col + ") AS " + col)
      }

    }
    sb.mkString(",")
  }

  def formatSelectCol(colInfoMap: Map[String, Int], colName: String): String = {
    val sb = new ArrayBuffer[String]()
    if (colInfoMap.contains(colName)) {
      val colIndex = colInfoMap.get(colName)
      for (entry <- colInfoMap) {
        if (entry._2 == colIndex.get) {
          if (colName.endsWith("_cl")) {
            sb.append("confidence AS " + colName)
          } else {
            sb.append("prediction AS " + colName)
          }
        } else {
          val colType = ConfigurationManager.getString("profile." + entry._1 + ".type").toLowerCase
          if (colType.equals("int")) {
            sb.append("-1 AS " + entry._1)
          } else if (colType.equals("string")) {
            sb.append("'' AS " + entry._1)
          } else if (colType.equals("double")) {
            sb.append("-1.0 AS " + entry._1)
          } else {
            sb.append("NULL AS " + entry._1)
          }
        }

      }
    }
    sb.mkString(",")
  }

  def readData2TempViewTable(spark: SparkSession, tagName: String, day: String): Unit = {
    // println("SAVING DATA TO TEMPVIEW_TABLE_" + tagName)
    var tagFile = ConfigurationManager.getString("profile." + tagName + ".data").trim
    if (tagFile.startsWith("/") || tagFile.startsWith("hdfs://") || tagFile.startsWith("file://")) { // 文件
      tagFile = tagFile.replace("${day}", day)
      if (tagFile.endsWith(".json")) {
        spark.read.json(tagFile).createOrReplaceTempView("TEMPVIEW_TABLE_" + tagName)
      } else {
        spark.read.orc(tagFile).createOrReplaceTempView("TEMPVIEW_TABLE_" + tagName)
      }
    } else { // 表
      if (!tagName.endsWith("_cl")) {
        val sql =
          s"""
             |SELECT * FROM $tagFile WHERE day='$day' AND kind='$tagName'
         """.stripMargin
        val df = spark.sql(sql)
        println(sql)
        //  df.show()
        df.createOrReplaceTempView("TEMPVIEW_TABLE_" + tagName)
        df.createOrReplaceTempView("TEMPVIEW_TABLE_" + tagName + "_cl")
      }
    }
  }
}
