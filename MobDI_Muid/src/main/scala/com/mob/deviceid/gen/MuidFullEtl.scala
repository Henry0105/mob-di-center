package com.mob.deviceid.gen

import java.util

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

object MuidFullEtl {

  private val LOGGER: Logger = LoggerFactory.getLogger("MuidFullEtl")

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().appName("MuidFullEtl").getOrCreate()

    val dataSourceDF = spark.sql(
      s"""
         |select device_old,device_token,muid,token,ieid,mcid,snid,oiid,asid,sysver,factory,serdatetime
         |from mobdi_test.hugl_muid_full_android_only_etl
         |""".stripMargin)
    import spark.implicits._
    val df = dataSourceDF.rdd.map(row => {
      val device_old = row.getString(row.fieldIndex("device_old"))
      val device_token = row.getString(row.fieldIndex("device_token"))
      val muid = row.getString(row.fieldIndex("muid"))
      val token = row.getString(row.fieldIndex("token"))
      val ieid = row.getString(row.fieldIndex("ieid"))
      val mcid = row.getString(row.fieldIndex("mcid"))
      val snid = row.getString(row.fieldIndex("snid"))
      val oiid = row.getString(row.fieldIndex("oiid"))
      val asid = row.getString(row.fieldIndex("asid"))
      val sysver = row.getString(row.fieldIndex("sysver"))
      val factory = row.getString(row.fieldIndex("factory"))
      val serdatetime = row.getString(row.fieldIndex("serdatetime"))
      val key = (device_old, device_token, muid, token)
      val value = List(ieid, mcid, snid, oiid, asid, sysver, factory, serdatetime)
      (key, value)
    }).groupByKey()
    val notEqDataSourceDF = df.filter(_._2.size >= 20000).map(t => {
      val devices = t._1
      val rows = t._2
      val results =
        ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String)]()
      rows.foreach(row => {
        results += Tuple12(devices._1, devices._2, devices._3, devices._4
          , row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7))
      })
      results
    }).flatMap(f => f).toDF("device_old"
      , "device_token", "muid", "token", "ieid"
      , "mcid", "snid", "oiid", "asid", "sysver", "factory", "serdatetime")
    val doEqDataSourceDF = df.filter(_._2.size < 20000)
    // 参与比较的逻辑
    val doEqRowsDF = doEqDataSourceDF.map(row => {
      val devices = row._1
      val doEqIdsAndSysFacTime = row._2.toList
      val needToRemoveIndex = mutable.Set[Int]()
      val inner = new Breaks
      for (i <- 0 to doEqIdsAndSysFacTime.size - 2) {
        inner.breakable {
          for (j <- i + 1 until doEqIdsAndSysFacTime.size) {
            val curIds = doEqIdsAndSysFacTime(i).dropRight(3)
            val eqIds = doEqIdsAndSysFacTime(j).dropRight(3)

            if (isContains(eqIds, curIds)) {
              // 比较行包含当前行,删除当前行
              needToRemoveIndex.add(i)
              inner.break()
            } else if (isContains(curIds, eqIds)) {
              // 当前行包含比较行,删除比较行
              needToRemoveIndex.add(j)
            } else {
              // 互相不包含,都保留

            }
          }
        }
      }

      // 比较完成,删除"重复"行数据
      val doEqIdsAndSysFacTimeJava = new util.ArrayList[List[String]](doEqIdsAndSysFacTime.asJava)
      val needToRetainElement = new util.ArrayList[List[String]]()
      for (m <- 0 until doEqIdsAndSysFacTimeJava.size()) {
        var retainFlag = true
        needToRemoveIndex.foreach(k => {
          if (m == k) {
            retainFlag = false
          }
        })
        if (retainFlag) {
          needToRetainElement.add(doEqIdsAndSysFacTimeJava.get(m))
        }
        retainFlag = true
      }
      val results =
        ArrayBuffer[(String, String, String, String, String, String, String, String, String, String, String, String)]()
      needToRetainElement.forEach(row => {
        results += Tuple12(devices._1, devices._2, devices._3, devices._4
          , row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7))
      })
      results
    }).flatMap(f => f).toDF("device_old"
      , "device_token", "muid", "token", "ieid"
      , "mcid", "snid", "oiid", "asid", "sysver", "factory", "serdatetime")
    // 不参与比较的合并起来
    doEqRowsDF.unionByName(notEqDataSourceDF).createOrReplaceTempView("tmp_result_table")
    spark.sql("drop table if exists mobdi_test.hugl_muid_full_remove_mutil_records_v2")
    spark.sql(
      s"""
         |create table mobdi_test.hugl_muid_full_remove_mutil_records_v2 stored as orc as
         |select device_old,device_token,muid,token,ieid,mcid,snid,oiid,asid,sysver,factory,serdatetime
         |from tmp_result_table
         |""".stripMargin)
  }

  def isContains(current : List[String], equal : List[String]): Boolean = {
    if (current == null || equal == null || current.size != equal.size) return false
    for(i <- current.indices) {
      if (current(i) != equal(i) && equal(i) != "" && equal(i) != null) return false
    }
    true
  }
}
