package com.youzu.mob.dailyrun

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ModelProfileTableMerge {

  def addColumn(spark: SparkSession, tableName: String, columnMap: Map[String, String]): Unit = {
    val sb = StringBuilder.newBuilder
    for (entry <- columnMap) {
      sb.append(entry._1).append(" ").append(entry._2).append(", ")
    }
    if (sb.nonEmpty) {
      val sqlStr = s"""ALTER TABLE $tableName ADD COLUMNS (${sb.substring(0, sb.length - 2)})"""
      try {
        spark.sql(sqlStr)
      } catch {
        case e: Exception => throw
          new RuntimeException(
            s"Needs to update spark version 2.2.0 or above.\nMore details at " +
              s"https://issues.apache.org/jira/browse/SPARK-19261\nSQL DETAIL: $sqlStr",
            e)
      }
    }
  }

  def seq2Str(tableName: String, seq: Seq[String]): String = {
    if (seq.isEmpty) {
      return ""
    }
    val separator = ", "
    val sb = StringBuilder.newBuilder
    for (s <- seq.sorted) {
      sb.append(tableName + "." + s).append(separator)
    }
    sb.substring(0, sb.length - 2)
  }

  def readData2TempViewTable(spark: SparkSession, tagName: String, day: String): Unit = {
    println("SAVING DATA TO TEMPVIEW_TABLE_" + tagName)
    var tagFile = ConfigurationManager.getString("profile." + tagName + ".data").trim
    if (tagFile.startsWith("/") || tagFile.startsWith("hdfs://") || tagFile.startsWith("file://")) {
      tagFile = tagFile.replace("${day}", day)
      if (tagFile.endsWith(".json")) {
        spark.read.json(tagFile).createOrReplaceTempView("TEMPVIEW_TABLE_" + tagName)
      } else {
        spark.read.orc(tagFile).createOrReplaceTempView("TEMPVIEW_TABLE_" + tagName)
      }
    } else {
      spark.
        sql(s"SELECT * FROM $tagFile WHERE day='$day' AND kind='$tagName'")
        .createOrReplaceTempView("TEMPVIEW_TABLE_" + tagName)
    }
  }

  def formatMAXCol(targetTableCols: Seq[String], diffCols: Seq[String]): String = {
    val sb = StringBuilder.newBuilder
    for (col <- targetTableCols) {
      sb.append("MAX(" + col + ") AS ").append(col).append(", ")
    }
    for (col <- diffCols) {
      sb.append("MAX(" + col + ") AS ").append(col).append(", ")
    }
    sb.substring(0, sb.length - 2)
  }

  def formatSelectCol(colInfoMap: Map[String, Int], colName: String): String = {
    val sb = StringBuilder.newBuilder
    if (colInfoMap.contains(colName)) {
      val colIndex = colInfoMap.get(colName)
      for (entry <- colInfoMap) {
        if (entry._2 == colIndex.get) {
          sb.append("prediction AS " + colName).append(", ")
        } else {
          sb.append("NULL AS " + entry._1 + ", ")
        }
      }

    }
    sb.substring(0, sb.length - 2)
  }


  def formatReviseCol(tableName: String, allCols: Seq[String],
    reviseCols: Seq[String], reviseMap: Map[String, String]): String = {
    val sb = StringBuilder.newBuilder
    for (col <- allCols) {
      if (reviseCols.contains(col) && reviseMap.contains(col)) {
        val rule = reviseMap.get(col)
        sb.append(rule.get).append(" AS ").append(col).append(", \n")
      } else {
        sb.append(tableName + "." + col).append(" AS ").append(col).append(", \n")
      }
    }
    sb.substring(0, sb.length - 3)
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val conf = new SparkConf().setAppName("ModelProfileTableMerge")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    if (args.length != 3 || args(0).isEmpty || args(1).isEmpty || args(2).isEmpty) {
      throw new IllegalArgumentException("Parameter number error!")
    }

    val searchDay = args(0)
    val insertDay = args(1)
    val configFile = args(2)

    ConfigurationManager.init(configFile)
    val targetTable = ConfigurationManager.getString("table.profile.name")
    val targetPath = ConfigurationManager.getString("table.profile.path")
    val reviseTable = ConfigurationManager.getString("table.revise.name")
    val revisePath = ConfigurationManager.getString("table.revise.path")

    val tagSeq = ConfigurationManager.getTagSeq
    println(tagSeq)
    ConfigurationManager.checkData(tagSeq)

    val targetTableCols = spark.sql(
      s"SELECT * FROM $targetTable LIMIT 1"
    ).columns.toSeq.diff(Seq("device", "day"))

    var colInfoMap: Map[String, Int] = Map()
    var colIndex = 1
    for (col <- targetTableCols) {
      colInfoMap += (col -> colIndex)
      colIndex = colIndex + 1
    }

    val diffCols = tagSeq.diff(targetTableCols).diff(Seq("")).distinct

    val allCols = targetTableCols.union(diffCols).distinct

    if (diffCols.nonEmpty) {
      var diffColMap: Map[String, String] = Map()
      for (col <- diffCols) {
        diffColMap += (col -> ConfigurationManager.getString("profile." + col + ".type"))
        colInfoMap += (col -> colIndex)
        colIndex = colIndex + 1
      }
      addColumn(spark, targetTable, diffColMap)
      addColumn(spark, reviseTable, diffColMap)

    }


    val selectColSql = StringBuilder.newBuilder
    for (tagName <- tagSeq) {
      readData2TempViewTable(spark, tagName, insertDay)
      selectColSql.append("SELECT device, ")
        .append(formatSelectCol(colInfoMap, tagName)).append(
        " FROM TEMPVIEW_TABLE_" + tagName).append(" UNION ALL \n")
    }

    val unionSql =
      s"""
         |SELECT
         |device, ${formatMAXCol(targetTableCols, diffCols)}
         |FROM (
         |${selectColSql.substring(0, selectColSql.length - 12)}
         |) t
         |GROUP BY t.device
       """.stripMargin

    println(unionSql)
    val unionDF = spark.sql(unionSql)
    unionDF.createOrReplaceTempView("UNIONED_TABLE")
    println("unionDF=" + unionDF.count())


    println(allCols)

    val updateSql =
      s"""
         |INSERT OVERWRITE TABLE $targetTable PARTITION (day='$insertDay')
         |SELECT
         |device, ${seq2Str("RANK_TABLE", allCols)}
         |FROM
         |(
         |  SELECT
         |  device, ${seq2Str("UPDATED_TABLE", allCols)},
         |  row_number() over (PARTITION BY device ORDER BY ver DESC) AS rank
         |  FROM (
         |    SELECT device, ${seq2Str("t2", allCols)}, 2 AS ver FROM UNIONED_TABLE t2
         |    UNION ALL
         |    SELECT device, ${seq2Str("t1", allCols)}, 1 AS ver FROM $targetTable t1 WHERE day='$searchDay'
         |  ) UPDATED_TABLE
         |) RANK_TABLE
         |WHERE rank=1
       """.stripMargin

    println(updateSql)
    spark.sql(updateSql)

    val reviseMap = ConfigurationManager.getReviseMap

    val reviseR1eq = ConfigurationManager.getString("reviseR1").split(",").map(str => str.trim).toSeq

    val reviseR1JoinSql = StringBuilder.newBuilder
    for (reviseCol <- reviseR1eq) {
      if (reviseMap.contains(reviseCol)) {
        reviseR1JoinSql.
          append(s"LEFT JOIN TEMPVIEW_TABLE_${reviseCol} ${reviseCol} ON (${reviseCol}.device=UNIONED_TABLE.device)\n")
      }
    }

    val revisedR1Sql =
      s"""
         |SELECT
         |UNIONED_TABLE.device,
         |${formatReviseCol("UNIONED_TABLE", allCols, reviseR1eq, reviseMap)}
         |FROM UNIONED_TABLE
         |${reviseR1JoinSql.toString()}
       """.stripMargin
    println(revisedR1Sql)
    spark.sql(revisedR1Sql).createOrReplaceTempView("REVISED_V1_TABLE")

    val reviseR2Seq = ConfigurationManager.getString("reviseR2").split(",").map(str => str.trim).toSeq

    val reviseR2JoinSql = StringBuilder.newBuilder
    for (reviseCol <- reviseR2Seq) {
      if (reviseMap.contains(reviseCol)) {
        reviseR2JoinSql.append(
          s"LEFT JOIN TEMPVIEW_TABLE_${reviseCol} ${reviseCol} ON (${reviseCol}.device=REVISED_V1_TABLE.device)\n")
      }
    }

    val revisedR2Sql =
      s"""
         |INSERT OVERWRITE TABLE $reviseTable PARTITION (day='$insertDay')
         |SELECT
         |REVISED_V1_TABLE.device,
         |${formatReviseCol("REVISED_V1_TABLE", allCols, reviseR2Seq, reviseMap)}
         |FROM REVISED_V1_TABLE
         |${reviseR2JoinSql.toString()}
       """.stripMargin
    println(revisedR2Sql)

    spark.sql(revisedR2Sql)

    spark.stop()

  }
}
