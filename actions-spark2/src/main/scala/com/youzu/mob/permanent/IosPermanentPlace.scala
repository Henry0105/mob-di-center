package com.youzu.mob.permanent

import com.youzu.mob.utils.Constants._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object IosPermanentPlace {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val day = args(0)
    val p30day = args(1)
    val spark = SparkSession
      .builder()
      .appName("permanent_place")
      .enableHiveSupport()
      .getOrCreate()
    val df1 = spark.sql(
      s"""
         |select b.idfa,a.country,a.province,a.city,a.day
         |from (
         |      select device,day,country,province,city
         |      from(
         |      select device,day,country,province,city,ROW_NUMBER() OVER(PARTITION BY device ORDER BY day desc) as rank
         |      from $DWS_DEVICE_IP_INFO_DI
         |      where plat=2 and day<=$day and day >=$p30day
         |      )
         |      where rank=1
         |      )a
         |join (select device,idfas as idfa
         |      from $IOS_ID_MAPPING_FULL_VIEW lateral view explode(split(idfa,",")) t as idfas
         |      where idfas<>''and idfas<>'00000000-0000-0000-0000-000000000000'
         |      ) b
         |on a.device=b.device
       """.stripMargin)
    // df1.show()
    df1.createOrReplaceTempView("tmp_idfa_result")

    val df2 = spark.sql(
      s"""
         |select a.idfa,b.country_code as country,b.province_code as province,a.city,b.country_cn,b.province_cn,b.city_cn,count(a.city) as cnt
         |from (select * from tmp_idfa_result where city<>'') a
         |join dm_sdk_mapping.map_city_sdk b
         |on a.city=b.city_code
         |group by a.idfa,b.country_code,b.province_code,a.city,b.country_cn,b.province_cn,b.city_cn
         |union all
         |select c.idfa,d.country_code as country,c.province,c.city,d.country_cn,d.province_cn,'未知' as city_cn,count(c.province) as cnt
         |from (select * from tmp_idfa_result where province<>'' and city='') c
         |join dm_sdk_mapping.map_city_sdk d
         |on c.province=d.province_code
         |group by c.idfa,d.country_code,c.province,c.city,d.country_cn,d.province_cn
         |union all
         |select e.idfa,e.country,e.province,e.city,f.country_cn,'未知' as province_cn,'未知' as city_cn,count(e.country) as cnt
         |from (select * from tmp_idfa_result where country<>'' and province='' and city='') e
         |join dm_sdk_mapping.map_city_sdk f
         |on e.country=f.country_code
         |group by e.idfa,e.country,e.province,e.city,f.country_cn
       """.stripMargin)
    df2.createOrReplaceTempView("tmp_city")
    spark.sql(
      s"""
         |insert overwrite table $IOS_PERMANENT_PLACE partition(day=$day)
         |select idfa,country as permanent_country,province as permanent_province,city as permanent_city,
         |        country_cn as permanent_country_cn,province_cn as permanent_province_cn,city_cn as permanent_city_cn
         |from
         |  (select a.idfa,a.country,a.province,a.city,a.country_cn,a.province_cn,a.city_cn,
         |        ROW_NUMBER() OVER(PARTITION BY a.idfa ORDER BY a.cnt desc,b.day desc) as rank
         |  from tmp_city a
         |  join tmp_idfa_result b
         |  on a.idfa=b.idfa and a.city=b.city and a.province=b.province and a.country=b.country)
         |where rank=1
       """.stripMargin)
  }
}
