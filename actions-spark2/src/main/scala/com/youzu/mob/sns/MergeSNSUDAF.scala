package com.youzu.mob.sns

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MergeSNSUDAF(_snsplat: String, _snsuid: String, _maxday: String)
  extends UserDefinedAggregateFunction {
  private val length = SNSList.snsuidList.length
  val schema = StructType(Array(
    StructField(_snsplat, IntegerType, nullable = true),
    StructField(_snsuid, StringType, nullable = true),
    StructField(_maxday, StringType, nullable = true)))

  override def inputSchema: StructType = schema

  override def bufferSchema: StructType = StructType(SNSList.snsuidList.fields)

  override def dataType: DataType = {
    SNSList.snsuidList
  }

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    for (i <- 0 until length) {
      buffer(i) = ""
    }
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val snsplat = input.getAs[Int](0)
    val snsuid = input.getAs[String](1).trim
    val maxday = input.getAs[String](2)
    val kv = snsuid + "=" + maxday

    val idx = snsplat match {
      case x if x >= 994 =>
        val offset = 999 - x
        length - offset
      case y => y - 1
    }

    if (idx >= 0 && idx < length) {
      val content = buffer(idx)
      content match {
        case "" => buffer(idx) = kv
        case c if c.asInstanceOf[String].contains(snsuid) => buffer(idx) = c
        case c => buffer(idx) = c + "," + kv
      }
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    for (idx <- 0 until SNSList.snsuidList.length) {
      buffer1(idx) = (buffer1.getAs[String](idx), buffer2.getAs[String](idx)) match {
        case ("", "") => ""
        case (c1, "") => c1
        case ("", c2) => c2
        case (c1, c2) => (c1.split(",") ++ c2.split(",")).toSet.mkString(",")
      }
    }
  }

  override def evaluate(buffer: Row): Any = {
    SNSList.array2SNSList(buffer.toSeq.map(str => MergeSNSUDAF.sortSnsplatByDay(str.toString)).toArray, 0)
  }
}

object MergeSNSUDAF {
  def sortSnsplatByDay(snsplat: String): String = {
    snsplat match {
      case "" => ""
      case x =>
        x.split(",")
          .map(str => {
            val arr = str.split("=", -1)
            if (arr.length < 2) {
              (arr(0), "")
            } else {
              (arr(0), arr(1))
            }
          })
          .sortBy(_._2)(Ordering[String].reverse)
          .map(tuple => tuple._1 + "=" + tuple._2)
          .mkString(",")
    }
  }
}