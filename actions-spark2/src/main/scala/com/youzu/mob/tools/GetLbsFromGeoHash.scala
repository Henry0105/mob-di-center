package com.youzu.mob.tools

import org.apache.spark.sql.SparkSession
import com.youzu.mob.utils.Constants._

object GetLbsFromGeoHash {
  def main(args: Array[String]): Unit = {
    val table = args(0)
    val outtable = args(2)
    val fields = args(1)
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    spark.sql(
      s"""
        |cache table mapping_tmp as
        |select * from $GEOHASH6_AREA_MAPPING
      """.stripMargin)
    spark.sql(
      s"""
         |select d.*,g.province_code as province,g.city_code as city,
         |        g.area_code as area,'' as street
         | from
         | (select ${fields} from ${table}  mm )d
         | left join mapping_tmp g
         |  on g.geohash_6_code = substring(d.geohash,1,6)
      """.stripMargin).registerTempTable("geohash6_tmp")
    spark.sql(s"DROP TABLE IF EXISTS ${outtable}")
    spark.sql(
      s"""
        |create table ${outtable} stored as orc as
        |select * from geohash6_tmp where province is not null
        | union all
        | select m.*,c.province_code as province,c.city_code as city,
        |        c.area_code as area,'' as street from
        | (
        |  select ${fields} from
        |  geohash6_tmp t where t.province is null
        | )m
        | left join
        | $GEOHASH8_LBS_INFO_MAPPING c
        | on c.geohash_8_code = m.geohash
      """.stripMargin)
  }
}
