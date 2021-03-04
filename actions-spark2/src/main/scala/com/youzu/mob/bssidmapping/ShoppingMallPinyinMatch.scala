package com.youzu.mob.bssidmapping

import org.apache.spark.sql.SparkSession
import com.github.stuxuhai.jpinyin._

object ShoppingMallPinyinMatch {

  def main(args: Array[String]): Unit = {
    // 用拼音推断商场的ssid
    if (args.length != 2) {
      println(
        s"""
           |error number of input parameters,please check your input
           |parameters like:<day>,<database>
         """.stripMargin)
      System.exit(-1)
    }
    println(args.mkString(","))
    val day = args(0)
    val database = args(1)

    val spark = SparkSession
      .builder()
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val cityAreaWord = spark.sql(
      s"""
         |select city_code,area_list
         |from $database.city_name_combine_area_name
         |where day='$day'
       """.stripMargin)
    cityAreaWord
      .select("city_code", "area_list")
      .map(r => (r.getString(0), r.getString(1),
        PinyinHelper.convertToPinyinString(r.getString(1), "", PinyinFormat.WITHOUT_TONE),
        PinyinHelper.getShortPinyin(r.getString(1))))
      .toDF("city_code", "area_list", "pinyin_city_area", "pinyin_short_city_area")
      .createOrReplaceTempView("pinyin_city_area_tmp")

    val shoppingMallNameWord = spark.sql(
      s"""
         |select name
         |from $database.shopping_mall_ssid_calculate_base_info
         |where day='$day'
         |and regexp_extract(lower(ssid),'([a-z])',1)!=''
         |group by name
     """.stripMargin)
    shoppingMallNameWord.select("name")
      .map(r => (r.getString(0),
        PinyinHelper.convertToPinyinString(r.getString(0), "", PinyinFormat.WITHOUT_TONE),
        PinyinHelper.getShortPinyin(r.getString(0))))
      .toDF("name", "pinyin_name", "pinyin_short_name")
      .createOrReplaceTempView("pinyin_name_temp")

    // join得到商场名字拼音和简写拼音、所在城市地区的拼音和简写拼音
    spark.sql(
      s"""
         |insert overwrite table $database.shopping_mall_bssid_calculate_pinyin_base_info partition(day='$day')
         |select a.name,poi_lat,poi_lon,a.city_code,flag_mall,ssid,device_num,connect_num,active_days,bssid_num,flag,
         |       pinyin_name,pinyin_short_name,pinyin_city_area,pinyin_short_city_area
         |from
         |(
         |  select name,poi_lat,poi_lon,city_code,flag_mall,ssid,device_num,connect_num,active_days,bssid_num,flag
         |  from $database.shopping_mall_ssid_calculate_base_info
         |  where day='$day'
         |  and regexp_extract(lower(ssid),'([a-z])',1)!=''
         |) a
         |left join
         |pinyin_name_temp b on a.name=b.name
         |left join
         |pinyin_city_area_tmp c on a.city_code=c.city_code
       """.stripMargin)

    // 对ssid中的英文进行分段
    spark.sql(
      s"""
         |insert overwrite table $database.shopping_mall_bssid_calculate_pinyin_ssid_split_base_info partition(day='$day')
         |select name,poi_lat,poi_lon,city_code,flag_mall,ssid,device_num,connect_num,active_days,bssid_num,flag,
         |       pinyin_name,pinyin_short_name,pinyin_city_area,pinyin_short_city_area,ssid_en,ssid_split
         |from
         |(
         |  select name,poi_lat,poi_lon,city_code,flag_mall,ssid,device_num,connect_num,active_days,bssid_num,flag,
         |         pinyin_name,pinyin_short_name,pinyin_city_area,pinyin_short_city_area,
         |         regexp_replace(regexp_replace(lower(ssid),'(\\\\s)',''),'([^a-z])','-') as ssid_en
         |  from $database.shopping_mall_bssid_calculate_pinyin_base_info
         |  where day='$day'
         |) t
         |lateral view explode(split(ssid_en,'-')) b as ssid_split
         |where ssid_split != ''
       """.stripMargin)

    // 当用拼音全称来判断时，分段的ssid长度要>=4，商场名字拼音必须包含分段的ssid，所在城市地区的拼音不能包含分段的ssid
    spark.sql(
      s"""
         |insert overwrite table $database.shopping_mall_split_ssid_pinyin_match partition(day='$day')
         |select name,poi_lat,poi_lon,city_code,flag_mall,ssid,device_num,connect_num,active_days,bssid_num,flag,
         |       pinyin_name,pinyin_city_area,ssid_en,ssid_split
         |from $database.shopping_mall_bssid_calculate_pinyin_ssid_split_base_info
         |where day='$day'
         |and instr(pinyin_name,ssid_split)>0
         |and instr(pinyin_city_area,ssid_split)=0
         |and length(regexp_replace(ssid_split,'^[zcs]hu\\B|[aeoi]ng$$','0'))>=4
       """.stripMargin)

    // 当用拼音缩写来判断时，分段的ssid长度要>=3，商场名字拼音必须包含分段的ssid，所在城市地区的拼音不能包含分段的ssid
    spark.sql(
      s"""
         |insert overwrite table $database.shopping_mall_split_ssid_pinyin_short_match partition(day='$day')
         |select name,poi_lat,poi_lon,city_code,flag_mall,ssid,device_num,connect_num,active_days,bssid_num,flag,
         |       pinyin_short_name,pinyin_short_city_area,ssid_en,ssid_split
         |from $database.shopping_mall_bssid_calculate_pinyin_ssid_split_base_info
         |where day='$day'
         |and instr(pinyin_short_name,ssid_split)>0
         |and instr(pinyin_short_city_area,ssid_split)=0
         |and length(ssid_split)>=3
       """.stripMargin)

    // ssid不分段
    // 根据拼音全称来判断(名字的拼音是否在整段的ssid内)
    spark.sql(
      s"""
         |insert overwrite table $database.shopping_mall_ssid_pinyin_match partition(day='$day')
         |select name,poi_lat,poi_lon,city_code,flag_mall,ssid,device_num,connect_num,active_days,bssid_num,flag,
         |       pinyin_name,ssid_en,pinyin_name_en
         |from
         |(
         |  select name,poi_lat,poi_lon,city_code,flag_mall,ssid,device_num,connect_num,active_days,bssid_num,flag,
         |         pinyin_name,
         |         regexp_replace(lower(ssid),'([^a-z])','') as ssid_en,
         |         regexp_replace(lower(pinyin_name),'([^a-z])','') as pinyin_name_en
         |  from $database.shopping_mall_bssid_calculate_pinyin_base_info
         |  where day='$day'
         |)a
         |where instr(ssid_en,pinyin_name_en)>0
         |and length(pinyin_name_en)>=3
       """.stripMargin)

    // 根据拼音缩写来判断(名字的拼音是否在整段的ssid内)
    spark.sql(
      s"""
         |insert overwrite table $database.shopping_mall_ssid_pinyin_short_match partition(day='$day')
         |select name,poi_lat,poi_lon,city_code,flag_mall,ssid,device_num,connect_num,active_days,bssid_num,flag,
         |       pinyin_short_name,ssid_en,pinyin_short_name_en
         |from
         |(
         |  select name,poi_lat,poi_lon,city_code,flag_mall,ssid,device_num,connect_num,active_days,bssid_num,flag,
         |         pinyin_short_name,
         |         regexp_replace(lower(ssid),'([^a-z])','') as ssid_en,
         |         regexp_replace(lower(pinyin_short_name),'([^a-z])','') as pinyin_short_name_en
         |  from $database.shopping_mall_bssid_calculate_pinyin_base_info
         |  where day='$day'
         |)a
         |where instr(ssid_en,pinyin_short_name_en)>0
         |and length(pinyin_short_name_en)>=3
       """.stripMargin)
  }
}
