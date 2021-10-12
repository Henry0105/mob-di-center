package com.youzu.mob.label

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import scala.math.BigInt
import com.mob.dm.lldistance.O2OPoiTypeNew
import com.mob.dm.lldistance.enu.OutputOperationTypeEnu
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import com.mob.dm.lldistance.handler.LbsTypedBasedOnHBasePOIHandler
import com.youzu.mob.utils.Constants._

/**
 * @author xdzhang
 *         description 找到lbs标签数据中每个设备的POI信息
 *         1,找到sdk_lbs_daily表中所有经纬度的POI信息，保存到dm_sdk_mapping.lat_lon_poi_mapping中
 *         2，将sdk_lbs_daily与得到的poi信息关联，得到dm_sdk_master.device_lbs_poi_daily(其中保存了所有店铺类型)
 *         3，用device_lbs_poi_daily数据与餐饮店铺的mapping表(dm_sdk_mapping.catering_cate_mapping mapping)关联
 *         得到一个device一天中堂吃信息保存到dm_sdk_master.device_catering_dinein_detail
 *         createTime 2017-11-10
 */
object LbsPOITmp {
  def main(args: Array[String]) {
    val conf = new SparkConf().
      setAppName("Spark Application " + this.getClass.getSimpleName.stripSuffix("$") + s"-${args(0)}")
    val sc: SparkContext = new SparkContext(conf)
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")
    val isRerun = false
    val day = dateFormat.parse(args(0))
    val beforeNum = args(1).toInt
    val bday = getDaysLater(day, beforeNum)
    val partitons = args(7).toInt
    val lbs_tmp = args(2)
    val poi_tmp = args(3)
    val isHbase = args(10).toBoolean
    val filesnum = args(11).toInt
    hiveContext.sql(lbs_tmp).repartition(partitons).registerTempTable("sdk_lbs_tmp")
    hiveContext.sql(poi_tmp).repartition(partitons).registerTempTable("lbs_poi_tmp")
    hiveContext.cacheTable("sdk_lbs_tmp")
    hiveContext.cacheTable("lbs_poi_tmp")
    val exceptsql = args(4)
    hiveContext.sql(exceptsql).repartition(partitons).registerTempTable("except_tmp")
    hiveContext.cacheTable("except_tmp")
    var unionM = ""
    val inputLbsSql = "select lat,lon from except_tmp"
    val count = hiveContext.sql(s"${inputLbsSql} limit 10").count()

    println(s"exceptRdd.count() ==> ${count}")
    if (count > 0) {
      if (isHbase) {
        LbsTypedBasedOnHBasePOIHandler.lbsPoiJoin(
          hiveContext, inputLbsSql,
          "10.6.24.107,10.6.24.111,10.6.24.113", "poi_geohash",
          200.0, 1000,
          "udf_tmp", OutputOperationTypeEnu(1))
      } else {
        val inputPoiSQL = "SELECT g7,lat,lon,name,type,type_id FROM dm_mobdi_tmp.dw_base_poi_l1_geohash"
        O2OPoiTypeNew.lbsPoiJoin(
          hiveContext, inputPoiSQL, inputLbsSql, useMapJoin = true,
          100, OutputOperationTypeEnu.cacheTable, "udf_tmp", 200.0)
      }

      val add_poi = args(6)
      hiveContext.sql(add_poi)
      hiveContext.uncacheTable("udf_tmp")
      hiveContext.uncacheTable("except_tmp")
      unionM =
        """
          |select geohash_center5,geohash_center6,geohash_center7,geohash_center8,
          |    lat,lon,type_1,type_2,type_3,type_4,type_5,type_6,type_7,type_8,type_9,type_10 from dm_mobdi_tmp.udf_tmp
          |  union all
          |select geohash_center5,geohash_center6,geohash_center7,geohash_center8,
          |    lat,lon,type_1,type_2,type_3,type_4,type_5,type_6,type_7,type_8,type_9,type_10 from lbs_poi_tmp
        """.stripMargin
    } else {
      unionM =
        """
          |select geohash_center5,geohash_center6,geohash_center7,geohash_center8,
          |lat,lon,type_1,type_2,type_3,type_4,type_5,type_6,type_7,type_8,type_9,type_10 from lbs_poi_tmp
        """.stripMargin
    }

    hiveContext.sql(unionM).repartition(partitons).registerTempTable("unionM_tmp")
    hiveContext.uncacheTable("lbs_poi_tmp")
    hiveContext.cacheTable("unionM_tmp")

    for (i <- 0 to beforeNum) {
      val partition = getDaysLater(day, i)
      val tmp_sql = args(5).replace("#", partition)
      val insert_daily_sql = args(8)
      val insert_dinein_sql = args(9)
      hiveContext.sql(tmp_sql).registerTempTable("daily_tmp")
      hiveContext.sql(insert_daily_sql).registerTempTable("insert_daily")
      hiveContext.sql(insert_dinein_sql).registerTempTable("insert_dinein")
      hiveContext.sql("select * from insert_daily").registerTempTable("insert_daily_cache")
      hiveContext.sql("select * from insert_dinein").registerTempTable("insert_dinein_cahe")
      hiveContext.sql("select * from insert_dinein_cahe ").
        coalesce(filesnum).registerTempTable("insert_dinein_repartition")
      hiveContext.sql("select * from insert_daily_cache ")
        .coalesce(filesnum).registerTempTable("insert_daily_repartition")
      hiveContext.sql(
        s"""insert overwrite table $DWS_DEVICE_CATERING_DINEIN_DI partition(day=${partition})
           |select * from insert_dinein_repartition
         """.stripMargin)
      hiveContext.sql(
        s"""insert overwrite table $DWS_DEVICE_LBS_POI_10TYPE_DI partition(day=${partition})
           |select * from insert_daily_repartition
         """.stripMargin)
    }
    sc.stop()
  }

  def getDaysLater(dt: Date, interval: Int): String = {
    val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyMMdd")

    val cal: Calendar = Calendar.getInstance()
    cal.setTime(dt);

    cal.add(Calendar.DATE, -interval)
    val yesterday = dateFormat.format(cal.getTime())
    yesterday
  }
}