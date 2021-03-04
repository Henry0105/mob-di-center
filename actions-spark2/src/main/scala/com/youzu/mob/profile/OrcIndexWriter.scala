package com.youzu.mob.profile

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.fs._
import org.apache.hadoop.io.{NullWritable, Text}
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Days}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

case class OrcIndexWriter(
                           @transient spark: SparkSession,
                           table: String,
                           warehouse: String = "/user/hive/warehouse"
                         ) extends Logging {

  import spark.implicits._

  spark.udf.register("agg_fn", new AggFunction())
  spark.udf.register("map_agg_fn", new MapAggFunction())

  def write(day: String, prefix: String): Unit = {
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

    logger.info(
      s"""
         |
         |partitions:
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |${orcFile.partitions.mkString("\n")}
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |
      """.stripMargin)


    if (checkPath(s"$warehouse/$db.db/$tableName/day=$prefix$day")) {
      orcFile.map(_._2.toString).map { str =>
        if (str.split("\u0001").length != 3) {
          (null, null, 0)
        } else {
          val Array(device, rowNumber, fileName) = str.split("\u0001")
          (device, fileName, rowNumber.toInt)
        }

        //(str.split("\u0001")(0),str.split("\u0001")(2),str.split("\u0001")(1))
      }.toDF(
        "device", "file_name", "row_number"
      ).repartition(128).createOrReplaceTempView("t")

      sql(
        s"""
           |insert overwrite table $db.timewindow_online_profile_day_index partition(table='$table',day='$day')
           |select device,file_name,row_number from t
           |where file_name is not null and row_number is not null
      """.stripMargin)
    }


  }

  def aggregate(start: String, end: String, version: String): Unit = {
    val Array(db, tableName) = table.split("\\.")
    sql(
      s"""
         |insert overwrite table $db.index_profile_history_all partition(table='$table',version='$version')
         |select device,agg_fn(file_name,row_number,day) as feature_index
         |from $db.timewindow_online_profile_day_index
         |where table='$table' and day>=$start and day<=$end and row_number is not null and file_name is not null
         |group by device
      """.stripMargin)
  }

  def aggregate(versions: Seq[String], newVersion: String): Unit = {
    val Array(db, tableName) = table.split("\\.")
    sql(
      s"""
         |insert overwrite table $db.index_profile_history_all partition(table='$table',version='$newVersion')
         |select device,map_agg_fn(feature_index) as feature_index
         |  from $db.index_profile_history_all
         |  where version in (${versions.map(v => s"'$v'").mkString(",")}) and table='$table'
         |group by device
      """.stripMargin)
  }

  def daysBetween(start: String, end: String): Set[String] = {
    val s = DateTime.parse(start, DateTimeFormat.forPattern("yyyyMMdd"))
    val e = DateTime.parse(end, DateTimeFormat.forPattern("yyyyMMdd"))
    val days = Days.daysBetween(s, e).getDays
    (0 to days).map(s.plusDays).map(t => t.toString("yyyyMMdd")).toSet
  }

  def update(start: String, end: String, version: String, newVersion: String): Unit = {
    val Array(db, tableName) = table.split("\\.")
    val filters = daysBetween(start, end)

    def cleanFn: UDF1[Map[String, Row], Map[String, Row]] = new UDF1[Map[String, Row], Map[String, Row]] {
      override def call(fi: Map[String, Row]): Map[String, Row] = {
        fi.filter(p => !filters.contains(p._1))
      }
    }

    val schema = MapType(StringType, StructType(Array(
      StructField("file_name", StringType),
      StructField("row_number", LongType)
    )))

    spark.udf.register("clean_fn", cleanFn, schema)
    sql(
      s"""
         |insert overwrite table $db.index_profile_history_all partition(table='$table',version='$newVersion')
         |select device, map_agg_fn(feature_index) as feature_index
         |from (
         | select device,agg_fn(file_name,row_number,day) as feature_index
         | from $db.timewindow_online_profile_day_index
         | where day>=$start and day<=$end and table='$table'
         | and file_name is not null and row_number is not null
         | group by device
         |
         | union all
         |
         | select device,clean_fn(feature_index) as feature_index
         | from $db.index_profile_history_all
         | where version='$version' and table='$table'
         |) t
         |group by device
         |
      """.stripMargin)
  }

  //只能单年更新，不能跨年更新 ，更新到临时分区
  def updateTmp(start: String, end: String, prefix: String): Unit = {
    val Array(db, tableName) = table.split("\\.")
    val filters = daysBetween(start, end)
    val version = lastVersionYear(start, table)
    val newVersion = version.split("\\.")(0) + "." + (version.split("\\.")(1).toInt + 1).toString

    //先生成到备份的分区
    val tmp_newVersion = newVersion.substring(0, 5) + "0" + newVersion.substring(6, 9)

    implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)


    logger.info(
      s"""
         |
         |TmpUpdater job start
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |version:$version
         |newVersion:$newVersion
         |- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
         |
       """.stripMargin)

    def cleanFn: UDF1[Map[String, Row], Map[String, Row]] = new UDF1[Map[String, Row], Map[String, Row]] {
      override def call(fi: Map[String, Row]): Map[String, Row] = {
        fi.filter(p => !filters.contains(p._1))
      }
    }

    val schema = MapType(StringType, StructType(Array(
      StructField("file_name", StringType),
      StructField("row_number", LongType)
    )))

    spark.udf.register("clean_fn", cleanFn, schema)

    sql(
      s"""
         |insert overwrite table $db.index_profile_history_all partition(table='$table',version='$tmp_newVersion')
         |select device, map_agg_fn(feature_index) as feature_index
         |from (
         | select device,agg_fn(file_name,row_number,day) as feature_index
         | from $db.timewindow_online_profile_day_index
         | where day>='$start' and day<='$end' and table='$table'
         | group by device
         |
         | union all
         |
         | select device,clean_fn(feature_index) as feature_index
         | from $db.index_profile_history_all
         | where version='$version' and table='$table'
         |) t
         |group by device
      """.stripMargin)


  }

  //也就是tmp生成完的日表，和生成完的新分区，一次性上线，和updateTmp配合使用
  def atomicPartition(start: String, end: String, prefix: String): Unit = {
    val Array(db, tableName) = table.split("\\.")
    val filters = daysBetween(start, end)
    val version = lastVersion(table)
    val newVersion = version.split("\\.")(0) + "." + (version.split("\\.")(1).toInt + 1).toString

    //先生成到备份的分区
    val tmp_newVersion = newVersion.substring(0, 5) + "0" + newVersion.substring(6, 9)

    implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // 保持原子性,一次性将所有的分区从tmp_ 迁移到正式,如果失败，则回退所有迁移
    try {
      val startTime = System.currentTimeMillis()

      var flag = backupData(filters, version, prefix)
      if (flag) {
        sql(
          s"""
              alter table $db.index_profile_history_all partition (table='$table',version='$tmp_newVersion')
              rename to partition (table='$table',version='$newVersion')
          """)

        logger.info(
          s"""
             |backup success
             |costs:${System.currentTimeMillis() - startTime}
         """.stripMargin)
      } else {
        sql(s"ALTER TABLE $db.index_profile_history_all DROP " +
          s"IF EXISTS PARTITION(table='$table',version='$tmp_newVersion'")
      }

    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.error(s"error occurs, drop version[$tmp_newVersion]")
        sql(s"ALTER TABLE $db.index_profile_history_all DROP " +
          s"IF EXISTS PARTITION(table='$table',version='$tmp_newVersion')")
        sql(s"ALTER TABLE $db.index_profile_history_all DROP " +
          s"IF EXISTS PARTITION(table='$table',version='$newVersion')")
    }

  }

  def indexOnline(start: String, end: String, prefix: String): Unit = {
    val Array(db, tableName) = table.split("\\.")
    val filters = daysBetween(start, end)
    val version = lastVersion(table)
    val newVersion = version.split("\\.")(0) + "." + (version.split("\\.")(1).toInt + 1).toString

    //先生成到备份的分区
    val tmp_newVersion = newVersion.substring(0, 5) + "0" + newVersion.substring(6, 9)

    implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    // 保持原子性,一次性将所有的分区从tmp_ 迁移到正式,如果失败，则回退所有迁移
    try {
      val startTime = System.currentTimeMillis()


      sql(
        s"""
            alter table $db.index_profile_history_all partition (table='$table',version='$tmp_newVersion')
            rename to partition (table='$table',version='$newVersion')
        """)

      logger.info(
        s"""
           |backup success
           |costs:${System.currentTimeMillis() - startTime}
       """.stripMargin)


    } catch {
      case ex: Exception =>
        ex.printStackTrace()
        logger.error(s"error occurs, drop version[$tmp_newVersion]")
        sql(s"ALTER TABLE $db.index_profile_history_all DROP " +
          s"IF EXISTS PARTITION(table='$table',version='$tmp_newVersion')")
        sql(s"ALTER TABLE $db.index_profile_history_all DROP " +
          s"IF EXISTS PARTITION(table='$table',version='$newVersion')")
    }

  }

  //只更新几天，用于补数据，直接rename分区
  def updateDays(days: String, prefix: String): Unit = {
    val Array(db, tableName) = table.split("\\.")
    val filters = days.split(",").toSet
    val version = lastVersionYear(days.split(",")(0), table)
    val newVersion = version.split("\\.")(0) + "." + (version.split("\\.")(1).toInt + 1).toString

    def cleanFn: UDF1[Map[String, Row], Map[String, Row]] = new UDF1[Map[String, Row], Map[String, Row]] {
      override def call(fi: Map[String, Row]): Map[String, Row] = {
        fi.filter(p => !filters.contains(p._1))
      }
    }

    val schema = MapType(StringType, StructType(Array(
      StructField("file_name", StringType),
      StructField("row_number", LongType)
    )))

    spark.udf.register("clean_fn", cleanFn, schema)
    sql(
      s"""
         |insert overwrite table $db.index_profile_history_all partition(table='$table',version='$newVersion')
         |select device, map_agg_fn(feature_index) as feature_index
         |from (
         | select device,agg_fn(file_name,row_number,day) as feature_index
         | from $db.timewindow_online_profile_day_index
         | where day in ($days) and table='$table'
         | group by device
         |
         | union all
         |
         | select device,clean_fn(feature_index) as feature_index
         | from $db.index_profile_history_all
         | where version='$version' and table='$table'
         |) t
         |group by device
      """.stripMargin)
    var flag = backupData(filters, version, prefix)

  }

  //单纯的rename分区

  def renamePartition(start: String, end: String, prefix: String): Boolean = {
    val Array(db, tableName) = table.split("\\.")
    val filters = daysBetween(start, end)
    val version = lastVersion(table)
    //val newVersion = (version.toInt + 1).toString
    implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var result = false
    if (StringUtils.isNotBlank(prefix)) {

      val mvFilters = new ArrayBuffer[String]()
      try {
        filters.toList.sorted.foreach(day => {
          mvFilters += day
          val flag1 = checkPath(s"$warehouse/$db.db/$tableName/day=$day")
          val flag2 = checkPath(s"$warehouse/$db.db/$tableName/day=$prefix$day")
          if (flag1 && flag2) {
            mv(day, s"bk_$day")
            mv(s"$prefix$day", day)
          } else if (flag2) {
            mv(s"$prefix$day", day)
          }

        })
        result = true
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          logger.error(
            s"""
               |error occurs, revert with mvFilters[${mvFilters.mkString("|")}]
             """.stripMargin)
          mvFilters.toList.sorted.foreach(day => {

            mv(day, s"$prefix$day")
            mv(s"bk_$day", day)
          })

      }
      logger.info(s"backup success with filters[${filters.mkString("|")}]")

    } else {
      logger.info(s"backup skip cause of empty prefix[$prefix]")
    }
    return result
  }


  def lastVersion(inputTable: String): String = {
    val Array(db, tableName) = table.split("\\.")

    logger.info(
      s"""
         |get index_profile_history_all last version
         """.stripMargin)
    //spark.sql(s"select version from $db.index_profile_history_all where table='$inputTable' group by version order
    // by version ").collect().last.getString(0)
    spark.sql(s"show partitions $db.index_profile_history_all partition (table='$inputTable')").orderBy("partition")
      .collect().last.getString(0).split("version=")(1)

  }

  def lastVersionYear(date: String, inputTable: String): String = {
    val Array(db, tableName) = table.split("\\.")
    val year = date.substring(0, 4)
    logger.info(
      s"""
         |get index_profile_history_all last year version
         """.stripMargin)
    // spark.sql(s"select version from $db.index_profile_history_all where table='$inputTable' group by version order
    // by version ").collect().last.getString(0)
    val version = sql(s"show partitions $db.index_profile_history_all").collect().map { r =>
      r.getString(0)
    }.filter(_.contains(table + "/")).filter(_.contains(year.toString)).map(_.split("=")(2)).max

    version
  }

  def backupData(filters: Set[String], version: String, prefix: String): Boolean = {
    val Array(db, tableName) = table.split("\\.")
    implicit val fs: FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    var result = false
    if (StringUtils.isNotBlank(prefix)) {

      val mvFilters = new ArrayBuffer[String]()
      try {
        filters.toList.sorted.foreach(day => {
          mvFilters += day
          val flag1 = checkPath(s"$warehouse/$db.db/$tableName/day=$day")
          val flag2 = checkPath(s"$warehouse/$db.db/$tableName/day=$prefix$day")
          if (flag1 && flag2) {
            //清理下bk的数据
            sql(
              s"""
                 |alter table $table drop  if exists partition (day='bk_$day')
       """.stripMargin)

            mv(day, s"bk_$day")
            mv(s"$prefix$day", day)
          } else if (flag2) {
            mv(s"$prefix$day", day)
          }

        })
        result = true
      } catch {
        case ex: Exception =>
          ex.printStackTrace()
          logger.error(
            s"""
               |error occurs, revert with mvFilters[${mvFilters.mkString("|")}]
             """.stripMargin)
          mvFilters.toList.sorted.foreach(day => {

            mv(day, s"$prefix$day")
            mv(s"bk_$day", day)
          })

      }
      logger.info(s"backup success with filters[${filters.mkString("|")}]")

    } else {
      logger.info(s"backup skip cause of empty prefix[$prefix]")
    }
    return result
  }

  def mv(src: String, dst: String): Unit = {

    try {
      spark.sql(
        s"""
           |alter table $table PARTITION (day='$src') RENAME TO PARTITION (day='$dst')
       """.stripMargin)
    } catch {
      case ex: Exception => throw ex
    }


  }

  def checkPath(path: String)(implicit fs: FileSystem): Boolean = {
    val src = new Path(path)
    if (fs.exists(src)) {
      return true
    } else {
      return false
    }
  }


  class AggFunction() extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = {
      StructType(Array(
        StructField("file_name", StringType),
        StructField("row_number", LongType),
        StructField("day", StringType)
      ))
    }

    override def bufferSchema: StructType = new StructType().add(
      StructField("t", MapType(StringType, StructType(Array(
        StructField("file_name", StringType),
        StructField("row_number", LongType)
      ))))
    )

    override def dataType: MapType = MapType(StringType, StructType(Array(
      StructField("file_name", StringType),
      StructField("row_number", LongType)
    )))

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, mutable.HashMap[String, Row]())
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      try {
        val m = buffer.getMap[String, Row](0)
        if (input == null || input.anyNull) {
          println("input is null")
          logger.info("input is null")
        } else {
          val fileName = input.getString(0)
          val num = input.getLong(1)
          val day = input.getString(2)
          buffer.update(0, m.updated(day, Row(fileName, num)))
        }

      }
      catch {
        case ex: Exception =>
          logger.error("%s agg fail update".format(ex.printStackTrace()))
          val m = buffer.getMap[String, Row](0)
          buffer.update(0, m)
      }

    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      try {
        val m1 = buffer1.getMap[String, Row](0)
        val m2 = buffer2.getMap[String, Row](0)
        buffer1.update(0, m1 ++ m2)
      } catch {
        case ex: Exception => logger.error(s"agg fail merge")
      }

    }

    override def evaluate(buffer: Row): Any = {
      try {
        buffer.getMap[String, Row](0)
      } catch {
        case ex: Exception => logger.error(s"agg fail evaluate")
      }

    }
  }

  class MapAggFunction() extends UserDefinedAggregateFunction {
    override def inputSchema: StructType = new StructType().add(
      StructField("feature_index", MapType(StringType, StructType(Array(
        StructField("file_name", StringType),
        StructField("row_number", LongType)
      ))))
    )

    override def bufferSchema: StructType = new StructType().add(
      StructField("t", MapType(StringType, StructType(Array(
        StructField("file_name", StringType),
        StructField("row_number", LongType)
      ))))
    )

    override def dataType: MapType = MapType(StringType, StructType(Array(
      StructField("file_name", StringType),
      StructField("row_number", LongType)
    )))

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, mutable.HashMap[String, Row]())
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val m1 = buffer.getMap[String, Row](0)
      val m2 = input.getMap[String, Row](0)
      buffer.update(0, m1 ++ m2)
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val m1 = buffer1.getMap[String, Row](0)
      val m2 = buffer2.getMap[String, Row](0)
      buffer1.update(0, m1 ++ m2)
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getMap[String, Row](0)
    }
  }

}

object OrcIndexWriter {
  def main(args: Array[String]): Unit = {
    //val Array(start, end, inputTable, prefix) = args

    val start = args(0)
    val end = args(1)
    val inputTable = args(2)
    val prefix = args(3)
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index")
      .enableHiveSupport()
      .getOrCreate()
    val writer = new OrcIndexWriter(spark, inputTable)

    //生成聚合表的索引
    writer.daysBetween(start, end).toList.sorted.foreach { day =>
      println(s"day=>$prefix$day ...")
      writer.write(day, prefix)


      println(s"day=>$prefix$day finished\n\n\n")
    }

  }
}

object OrcIndexAggregator {
  def main(args: Array[String]): Unit = {
    println(args)
    val Array(start, end, version, inputTable) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index")
      .enableHiveSupport()
      .getOrCreate()
    new OrcIndexWriter(spark, inputTable).aggregate(
      start, end, version
    )
  }
}

object OrcIndexAggregator2 {
  def main(args: Array[String]): Unit = {
    println(args)
    val Array(versions, newVersion, inputTable) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index")
      .enableHiveSupport()
      .getOrCreate()
    new OrcIndexWriter(spark, inputTable).aggregate(
      versions.split(","), newVersion
    )
  }
}

object OrcIndexUpdater {
  def main(args: Array[String]): Unit = {
    val Array(start, end, version, newVersion, inputTable) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index")
      .enableHiveSupport()
      .getOrCreate()
    new OrcIndexWriter(spark, inputTable).update(
      start, end, version, newVersion
    )
  }
}

//更新一段的数据到临时分区,和atomicPartition配合使用
object OrcIndexTmpUpdater {
  def main(args: Array[String]): Unit = {
    val Array(start, end, inputTable, prefix) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index updater")
      .enableHiveSupport()
      .getOrCreate()

    new OrcIndexWriter(spark, inputTable).updateTmp(
      start, end, prefix
    )
  }
}

//一次性更新索引和rename分区
object OrcIndexDaysUpdater {
  def main(args: Array[String]): Unit = {
    val Array(days, inputTable, prefix) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index days update")
      .enableHiveSupport()
      .getOrCreate()
    new OrcIndexWriter(spark, inputTable).updateDays(
      days, prefix
    )
  }
}

//单纯rename分区
object OrcIndexRename {
  def main(args: Array[String]): Unit = {
    val Array(start, end, inputTable, prefix) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index updater")
      .enableHiveSupport()
      .getOrCreate()

    new OrcIndexWriter(spark, inputTable).renamePartition(
      start, end, prefix
    )
  }
}

//单纯的remove最新的索引分区和rename分区
object OrcIndexAtomic {
  def main(args: Array[String]): Unit = {
    val Array(start, end, inputTable, prefix) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index updater")
      .enableHiveSupport()
      .getOrCreate()

    new OrcIndexWriter(spark, inputTable).atomicPartition(
      start, end, prefix
    )
  }
}

object OrcIndexOnline {
  def main(args: Array[String]): Unit = {
    val Array(start, end, inputTable, prefix) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("orc index updater")
      .enableHiveSupport()
      .getOrCreate()

    new OrcIndexWriter(spark, inputTable).indexOnline(
      start, end, prefix
    )
  }
}