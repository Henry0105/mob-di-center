package com.youzu.mob.profile.tags_info_full.helper

import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.util.Locale

import com.youzu.mob.profile.tags_info_full.beans.{ProfileInfo, ProfileData}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.collection.mutable

/**
 * @author xlmeng
 */
object TagsGeneratorHelper {

  val kvSep: String = "\u0001"
  val pairSep: String = "\u0002"
  val pSep: String = "\u0003"
  val num2str: String = "num2str"

  def sql(spark: SparkSession, query: String): DataFrame = {
    println(
      s"""
         |<<<<<<
         |$query
         |>>>>>>
        """.stripMargin)
    spark.sql(query)
  }

  def registerNum2Str(spark: SparkSession): Unit = {
    // 注册一个处理科学计数法问题的udf
    val df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH))
    df.setMaximumFractionDigits(340) // 340 = DecimalFormat.DOUBLE_FRACTION_DIGITS
    val dfBC = spark.sparkContext.broadcast(df)
    spark.udf.register(num2str, (d: Double) => {
      dfBC.value.format(d)
    })
  }

  def getConfidenceProfileIds(spark: SparkSession, confidenceTable: String): Array[(Int, Int)] = {
    import spark.implicits._
    sql(spark,
      s"""
         |select profile_id, profile_version_id
         |from $confidenceTable
       """.stripMargin)
      .map(r => (r.getAs[Int]("profile_id"), r.getAs[Int]("profile_version_id")))
      .collect()
  }

  def buildMapStringFromFields[T <: ProfileData](arr: Array[T], kvSep: String): String = {
    arr.map(_.columnClause(kvSep)).mkString(",")
  }

  def valueToStr(dataType: String, value: String): String = {
    dataType match {
      case "string" => s"$value"
      case "double" => s"$num2str($value)"
      case _ => s"cast($value as string)"
    }
  }

  /** 必须是timewindow的表 */
  def getValue2IdMapping(profiles: Array[ProfileInfo]): Map[String, String] = {
    profiles.flatMap { p =>
      val reEqualOp = """feature='([^']+)'""".r.findFirstMatchIn(p.profileColumn)
      val reInOps = """feature in \((.*)\)""".r.findFirstMatchIn(p.profileColumn)
      if (reEqualOp.isDefined) {
        val value = reEqualOp.get.subgroups.head
        Array(value -> p.fullVersionId)
      } else if (reInOps.isDefined) {
        val values = reInOps.get.subgroups.head.replace("'", "").split(",")
        values.map(_ -> p.fullVersionId)
      } else {
        Array.empty[(String, String)]
      }
    }.toMap
  }

  ////////////////////////////////////////////////////////////////////
  //                             udf                                //
  ////////////////////////////////////////////////////////////////////
  def cleanConfidence(confMap: Map[String, Seq[String]], valueMap: Map[String, Seq[String]]):
  Map[String, Seq[String]] = {
    if (null == confMap || confMap.size < 1 || null == valueMap || valueMap.size < 1) {
      null
    } else {
      // 如果valueMap中没有这个key也去掉该记录
      val matchMap = valueMap.map { case (k, v) => (k, if (null == v || v.isEmpty) "" else v.head) }
      val tmp = confMap.filterKeys { k =>
        matchMap.get(k) match {
          case Some(x) if StringUtils.isBlank(x) => false
          case Some("-1") => false
          case None => false
          case _ => true
        }
      }
      if (tmp.isEmpty) {
        null
      } else {
        tmp
      }
    }
  }

  /**
   * 初次生成时候，也保证tags_like数据一致
   */
  def removeOldTagsLike(bc: Broadcast[Set[String]])(map: Map[String, Seq[String]]): Map[String, Seq[String]] = {
    if (map == null) {
      return null
    }

    val tagTfidProfileIdSet = bc.value
    val (inPar, noInPar) = map.partition { case (k, _) => tagTfidProfileIdSet.contains(k) }
    if (inPar.isEmpty) {
      noInPar
    } else {
      val lastTime = inPar.maxBy(_._2(1))._2(1)
      val lastInPar = inPar.filter { case (_, v) => v(1) == lastTime }
      lastInPar ++ noInPar
    }
  }

  class kvStr2map() extends UserDefinedAggregateFunction {
    override def inputSchema: StructType =
      StructType(StructField("kv", StringType) :: StructField("pairSep", StringType) ::
        StructField("kvSep", StringType) :: StructField("update_time", StringType) ::
        StructField("task_time", StringType) :: Nil)

    override def bufferSchema: StructType =
      StructType(StructField("ms", MapType(StringType, ArrayType(StringType))) :: Nil)

    override def dataType: DataType = MapType(StringType, ArrayType(StringType))

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0) = Map.empty[String, Seq[String]]

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      if (!input.isNullAt(0)) {
        val pairSep = input.getAs[String](1)
        val kvSep = input.getAs[String](2)
        val kvs = input.getAs[String](0).split(pairSep)
          .map(_.split(kvSep))
          .filter(kv => kv.length == 2 && StringUtils.isNotBlank(kv(1)))
        if (kvs.nonEmpty) {
          val updateTime = input.getAs[String](3)
          val taskTime = input.getAs[String](4)
          val ms2 = kvs.map(kv => kv(0) -> Seq(kv(1), updateTime, taskTime)).toMap
          if (buffer.isNullAt(0)) {
            buffer(0) = ms2
          } else {
            val ms1 = buffer.getAs[Map[String, Seq[String]]](0)
            buffer(0) = ms1 ++ ms2
          }
        }
      }
    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      if (!buffer2.isNullAt(0)) {
        val ms1 = buffer1.getAs[Map[String, Seq[String]]](0)
        val ms2 = buffer2.getAs[Map[String, Seq[String]]](0)
        buffer1(0) = ms1 ++ ms2
      }
    }

    override def evaluate(buffer: Row): Any = {
      val ms = buffer.getAs[Map[String, Seq[String]]](0)
      if (ms.isEmpty) null else ms
    }
  }

}

case class TablePartitionsManager(spark: SparkSession) {
  val tablePartitions: mutable.Map[String, Seq[String]] = mutable.Map.empty[String, Seq[String]]

  def getPartitions(tableName: String): Seq[String] = {
    if (tablePartitions.contains(tableName)) {
      tablePartitions(tableName)
    } else {
      import spark.implicits._
      val parts = TagsGeneratorHelper.sql(spark, s"show partitions $tableName").map(_.getString(0)).collect()
      tablePartitions.put(tableName, parts)
      parts
    }
  }
}
