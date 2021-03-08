package com.youzu.mob.appinstall

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

import scala.collection.mutable
case class MasterReservedTime(
                        @transient spark: SparkSession,
                        outputTable: String
                        ) {
  spark.udf.register("map_agg_fn", new MapAggFunction())

  class MapAggFunction() extends UserDefinedAggregateFunction{


    override def inputSchema: StructType = new StructType().add(
      StructField("pkg_date", MapType(StringType, StringType))
    )

    override def bufferSchema: StructType = new StructType().add(
      StructField("t", MapType(StringType, StringType))
    )

    override def dataType: MapType = MapType(StringType, StringType)

    override def deterministic: Boolean = true

    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, mutable.HashMap[String, String]())
    }

    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val m1 = buffer.getMap[String, String](0)
      val m2 = input.getMap[String, String](0)

      if(m1 == null && m2 == null) {
        buffer.update(0, mutable.HashMap[String, String]())
      } else if (m1 == null || m1.isEmpty ) {
        buffer.update(0, m2)
      } else if (m2 == null || m2.isEmpty ) {
        buffer.update(0, m1)
      } else if ( m1.isEmpty && m2.isEmpty ) {
        buffer.update(0, mutable.HashMap[String, String]())
      } else {
        buffer.update(0, m1 ++ m2.map(
          t =>
            t._1 ->
              (if (t._2 !=null && m1.getOrElse(t._1, t._2) !=null && t._2 < m1.getOrElse(t._1, t._2)) t._2
              else m1.getOrElse(t._1, t._2))
        )
        )
      }

    }

    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      val m1 = buffer1.getMap[String, String](0)
      val m2 = buffer2.getMap[String, String](0)

      if(m1 == null && m2 == null ) {
        buffer1.update(0, mutable.HashMap[String, String]())
      } else if (m1 == null || m1.isEmpty ) {
        buffer1.update(0, m2)
      } else if (m2 == null || m2.isEmpty ) {
        buffer1.update(0, m1)
      } else if ( m1.isEmpty && m2.isEmpty ) {
        buffer1.update(0, mutable.HashMap[String, String]())
      } else {
        buffer1.update(0, m1 ++ m2.map(
          t =>
            t._1 ->
              (if (t._2 !=null && m1.getOrElse(t._1, t._2) !=null && t._2 < m1.getOrElse(t._1, t._2)) t._2
              else m1.getOrElse(t._1, t._2))
        )
        )
      }
    }

    override def evaluate(buffer: Row): Any = {
      buffer.getMap[String, Row](0)
    }
  }

  def update(start: String, end: String, old_day: String, new_day: String): Unit = {
    spark.sql(
      s"""
        |insert overwrite table $DEVICE_RESERVED_TIME_INCR
        |select device,
        |case when size(collect_set(install_pkg))>0  then
        |str_to_map(
        |  concat_ws(',',
        |    collect_set(
        |    install_pkg
        |    )
        |  )
        |) else map() end as install_date,
        |case when size(collect_set(unstall_pkg))>0 then
        |str_to_map(
        |  concat_ws(',',
        |    collect_set(
        |    unstall_pkg
        |    )
        |  )
        |) else map() end as unstall_date
        |from(
        |select device,case when refine_final_flag=1 then concat_ws(':',pkg,day) else null end as install_pkg,
        |case when refine_final_flag=-1 then concat_ws(':',pkg,day) else null
        |end as unstall_pkg,day
        | from $DWS_DEVICE_INSTALL_APP_RE_STATUS_DI where day>=${start} and day<=${end} and refine_final_flag in(1,-1)
        |)master
        |group by device
      """.stripMargin)

    spark.sql(
      s"""
        |insert overwrite table ${outputTable} partition (day=${new_day})
        |select device,map_agg_fn(install_date) as install_date,map_agg_fn(unstall_date) as unstall_date from
        |(
        |select device,install_date,unstall_date from ${outputTable} where day=${old_day}
        |and (size(install_date)>0 or size(unstall_date)>0)
        |union all
        |select device,install_date,unstall_date from $DEVICE_RESERVED_TIME_INCR
        |)a group by device
      """.stripMargin
    )
  }
}

object MasterReservedTime {

  def main(args: Array[String]): Unit = {
    println(args)
    val Array(outputTable, start, end, old_day, new_day) = args
    lazy val spark: SparkSession = SparkSession
      .builder()
      .appName(s"Reserved_time_${start}_${end}_${old_day}")
      .enableHiveSupport()
      .getOrCreate()
    val sc = spark.sparkContext

    sc.setLogLevel("WARN")
    import spark.implicits._
    import spark.sql

    new MasterReservedTime(spark, outputTable).update(start, end, old_day, new_day)

  }

}
