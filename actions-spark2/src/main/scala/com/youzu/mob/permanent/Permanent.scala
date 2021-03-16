package com.youzu.mob.permanent

import com.youzu.mob.utils.Constants._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Permanent {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {

    val day = args(0)
    val p90day = args(1)

    val spark = SparkSession
      .builder()
      .appName("permanent")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    val df = spark.sql(s"select device,location,day from $DEVICE_LOCATION_CURRENT " +
      s"where day between $p90day and $day and plat='1'")

    //展开location，提取其中的 "country", "province", "city"
    df.mapPartitions(row => {
      row.flatMap(x => {
        val array = ArrayBuffer[(String, String, String, String, String)]()
        val device = x(0).toString
        val location = x(1).asInstanceOf[mutable.Seq[Map[String, String]]]
        val day = x(2).toString
        var country = ""
        var province = ""
        var city = ""
        for (map <- location) {
          country = map("country")
          province = map("province")
          city = map("city")
          array += ((device, country, province, city, day))
        }
        array.toIterator
      })

    }).toDF("device", "country", "province", "city", "day").createOrReplaceTempView("tmp_location_info")


    spark.sql(
      s"""
         |with tmp_location as(
         |select device,country,province,city
         |from tmp_location_info
         |group by device,country,province,city,day
         |),
         |tmp_info as(
         |select device
         |	  ,case when  country ='' or trim(lower(country)) ='null' then null else country end as country
         |	  ,case when  province ='' or trim(lower(province)) ='null' then null else province end  as province
         |	  ,case when  city ='' or trim(lower(city)) ='null' then null else city end as city
         |from tmp_location
         |),
         |tmp_normal as(
         |select device,
         |       country,
         |       province,
         |       city,
         |       count(*) as cnt,
         |       '1' as flag
         |from tmp_info
         |where city is not null and province is not null and  country is not null
         |group by device,country,province,city
         |having cnt>3
         |union all
         |select device,
         |       country,
         |       province,
         |       null as city,
         |       count(*) as cnt,
         |       '2' as flag
         |from tmp_info
         |where province is not null and country is not null
         |group by device,country,province
         |having cnt>3
         |union all
         |select device,country,null as province,null as city,count(*) as cnt,'3' as flag
         |from tmp_info
         |where country is not null
         |group by device,country
         |having cnt>3
         |),
         |tmp_max_times_location as(
         |select device,country,province,city,cnt
         |from(
         |select device,country,province,city,cnt,row_number() over(partition by device order by flag,cnt desc) as rank
         |from tmp_normal
         |)
         |where rank=1
         |),
         |tmp_location_list as(
         |select device,
         |       countrylist,
         |       case when array_contains(flaglist,'1') or array_contains(flaglist,'2') then provincelist else null end as provincelist,
         |       case when array_contains(flaglist,'1')  then citylist else null end as citylist
         |from(
         |select device,
         |       collect_set(country) as countrylist,
         |       collect_set(province) as provincelist,
         |       collect_set(city) as citylist,
         |       collect_set(flag) as flaglist
         |from tmp_normal
         |group by device
         |)
         |)
         |insert overwrite  table $RP_DEVICE_LOCATION_PERMANENT partition(day='$day')
         |select device,
         |       COALESCE(country,'') as country,
         |       countrycount,
         |       countrylist,
         |       COALESCE(province,'') as province,
         |       provincecount,
         |       provincelist,
         |       COALESCE(city,'') as city,
         |       citycount,
         |       citylist,
         |       max_days
         |from(
         |select a.device
         |	  ,a.country
         |	  ,size(b.countrylist) as countrycount
         |	  ,b.countrylist
         |	  ,a.province
         |	  ,case when b.provincelist is null then 0 else size(b.provincelist) end as provincecount
         |	  ,COALESCE(b.provincelist,array('')) as provincelist
         |	  ,a.city
         |	  ,case when b.citylist is null then 0 else size(b.citylist) end as citycount
         |	  ,COALESCE(b.citylist,array('')) as citylist
         |    ,a.cnt as max_days
         |from tmp_max_times_location a
         |inner join tmp_location_list b
         |on a.device=b.device
         |)tt
         |
       """.stripMargin)


  }

}
