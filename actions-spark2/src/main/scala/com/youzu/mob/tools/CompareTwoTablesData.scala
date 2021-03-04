package com.youzu.mob.tools

import org.apache.spark.sql.SparkSession

object CompareTwoTablesData {

  def main(args: Array[String]): Unit = {
    val tableOld = args(0)
    val tableNew = args(1)
    val condition = args(2)
    val col = args(3)
    // 比较时排除的字段
    var colExcept = ""
    if (args.length > 4) {
      colExcept = args(4)
    }
    val columnList = col.split(",")
    var columns = ""
    for (i <- columnList.indices) {
      columns += "cast(nvl(" + columnList(i) + ",'') as String),"
    }
    columns = columns.substring(0, columns.length - 1)
    println(tableOld)
    println(tableNew)
    println(columns)

    // 筛选排除字段
    val columnExList = colExcept.split(",")
    val columnsRefineSet = columnList.toSet diff columnExList.toSet
    var columnsRefine = ""
    if (colExcept != "") {
      for (c <- columnsRefineSet) {
        columnsRefine += "cast(nvl(" + c + ",'') as String),"
      }
      columnsRefine = columnsRefine.substring(0, columnsRefine.length - 1)
      println(columnsRefine)
    }

    val spark = SparkSession
      .builder()
      .appName(s"CompareTwoTablesData $tableOld")
      .enableHiveSupport()
      .getOrCreate()

    val oldCount = spark.sql(
      s"""
         |select count(1)
         |from $tableOld
         |where $condition
      """.stripMargin
    ).toDF.collect()(0)(0)
    val newCount = spark.sql(
      s"""
         |select count(1)
         |from $tableNew
         |where $condition
      """.stripMargin
    ).toDF.collect()(0)(0)

    spark.sql(
      s"""
         |select md5v
         |from
         |(
         |  select concat_ws(',', $columns) AS md5v
         |  from $tableOld
         |  where $condition
         |)t
         |group by md5v
         |
         |union all
         |
         |select md5v
         |from
         |(
         |  select concat_ws(',', $columns) AS md5v
         |  from $tableNew
         |  where $condition
         |)t
         |group by md5v
      """.stripMargin
    ).createOrReplaceTempView("md5_table")
    val compareResult = spark.sql(
      s"""
         |select cnt, count(1) AS total_count
         |from
         |(
         |  select count(1) AS cnt, md5v
         |  from md5_table
         |  group by md5v
         |) grouped
         |group by cnt
      """.stripMargin
    ).toDF().collect()
    println("旧表总count：" + oldCount)
    println("新表总count：" + newCount)
    println("比对结果：")
    var oldPercent = 100.toDouble
    var newPercent = 100.toDouble
    for (i <- compareResult.indices) {
      println(compareResult(i)(0) + "," + compareResult(i)(1))
      if (compareResult(i)(0).toString.equals("2")) {
        oldPercent = compareResult(i)(1).toString.toDouble / oldCount.toString.toDouble * 100
        newPercent = compareResult(i)(1).toString.toDouble / newCount.toString.toDouble * 100
        println("相同数在旧表占比：" + oldPercent + "%")
        println("相同数在新表表占比：" + newPercent + "%")
      }
    }

    if (colExcept != "") {
      spark.sql(
        s"""
           |select md5v
           |from
           |(
           |  select concat_ws(',', $columnsRefine) AS md5v
           |  from $tableOld
           |  where $condition
           |)t
           |group by md5v
           |
           |union all
           |
           |select md5v
           |from
           |(
           |  select concat_ws(',', $columnsRefine) AS md5v
           |  from $tableNew
           |  where $condition
           |)t
           |group by md5v
        """.stripMargin
      ).createOrReplaceTempView("md5_table_refine")
      val compareRefineResult = spark.sql(
        s"""
           |select cnt, count(1) AS total_count
           |from
           |(
           |  select count(1) AS cnt, md5v
           |  from md5_table_refine
           |  group by md5v
           |) grouped
           |group by cnt
        """.stripMargin
      ).toDF().collect()
      println("排除字段后比对结果：")
      oldPercent = 100.toDouble
      newPercent = 100.toDouble
      for (i <- compareRefineResult.indices) {
        println(compareRefineResult(i)(0) + "," + compareRefineResult(i)(1))
        if (compareRefineResult(i)(0).toString.equals("2")) {
          oldPercent = compareRefineResult(i)(1).toString.toDouble / oldCount.toString.toDouble * 100
          newPercent = compareRefineResult(i)(1).toString.toDouble / newCount.toString.toDouble * 100
          println("排除字段后相同数在旧表占比：" + oldPercent + "%")
          println("排除字段后相同数在新表表占比：" + newPercent + "%")
        }
      }
    }
  }
}

/*if (oldPercent < 90 || newPercent < 90) {
println("打印出数据不同的例子：")
println(col)
spark.sql(
  s"""
     |select count(1) AS cnt, md5v, collect_list($primaryKey)[0] as primary_key
     |from md5_table
     |group by md5v
     |having count(1)=1
     |limit 10
   """.stripMargin
).createOrReplaceTempView("key_table")
val differentResult = spark.sql(
  s"""
     |select 'old' as tableversion,concat_ws(',', $columns) AS md5v, told.$primaryKey
     |from $tableOld told
     |inner join
     |key_table on key_table.primary_key = told.$primaryKey
     |where told.$condition
     |
     |union all
     |
     |select 'new' as tableversion,concat_ws(',', $columns) AS md5v, tnew.$primaryKey
     |from $tableNew tnew
     |inner join
     |key_table on key_table.primary_key = tnew.$primaryKey
     |where tnew.$condition
   """.stripMargin
).toDF().collect()
for (i <- differentResult.indices) {
  println(differentResult(i))
}
}*/
