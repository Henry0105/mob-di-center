package com.youzu.mob.bssidmapping

import com.github.stuxuhai.jpinyin._
import org.apache.spark.sql.SparkSession

object HotelPinyinMatch {

  def main(args: Array[String]): Unit = {
    // 用拼音推断酒店的ssid
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

    // 对酒店名去除关键字后，取最后1-3位字符作为name_cut字段
    val shoppingMallNameWord = spark.sql(
      s"""
         |select name,
         |regexp_extract(
         |  regexp_extract(name_1,'(.*?)(商务|假日|度假|快捷|连锁|精品|主题|国际|便捷|豪华|品质|时尚|大?$$)',1),
         |  '(.{1,3})$$',1)
         |as name_cut
         |from
         |(
         |  select name,
         |  regexp_extract(
         |    name,
         |    '(.*?)(酒店|宾馆|旅馆|青旅|酒店|酒楼|客栈|民宿|招待所|旅社|公寓|住宿|客房|旅社|饭店|会馆|\\\\(|$$)',1)
         |  as name_1
         |  from $database.hotel_ssid_calculate_base_info
         |  where day='$day'
         |  and regexp_extract(lower(ssid),'([a-z])',1)!=''
         |  group by name
         |) t1
     """.stripMargin)
    shoppingMallNameWord.select("name", "name_cut")
      .map(r => (r.getString(0), r.getString(1),
        PinyinHelper.convertToPinyinString(r.getString(0), "", PinyinFormat.WITHOUT_TONE),
        PinyinHelper.convertToPinyinString(r.getString(1), "", PinyinFormat.WITHOUT_TONE),
        PinyinHelper.getShortPinyin(r.getString(0)),
        PinyinHelper.getShortPinyin(r.getString(1))))
      .toDF("name", "name_cut",
        "pinyin_name", "pinyin_cut_name", "pinyin_short_name", "pinyin_cut_short_name")
      .createOrReplaceTempView("pinyin_name_temp")

    // join得到酒店名字拼音和简写拼音、所在城市地区的拼音和简写拼音、name_cut拼音和简写拼音
    spark.sql(
      s"""
         |insert overwrite table $database.hotel_bssid_calculate_pinyin_base_info partition(day='$day')
         |select a.name,poi_lat,poi_lon,a.city_code,ssid,device_num,connect_num,active_days,bssid_num,
         |       pinyin_name,pinyin_cut_name,pinyin_short_name,pinyin_cut_short_name,
         |       pinyin_city_area,pinyin_short_city_area
         |from
         |(
         |  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num
         |  from $database.hotel_ssid_calculate_base_info
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
         |insert overwrite table $database.hotel_bssid_calculate_pinyin_ssid_split_base_info partition(day='$day')
         |select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         |       pinyin_name,pinyin_cut_name,pinyin_short_name,pinyin_cut_short_name,
         |       pinyin_city_area,pinyin_short_city_area,ssid_en,ssid_split
         |from
         |(
         |  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         |         pinyin_name,pinyin_cut_name,pinyin_short_name,pinyin_cut_short_name,
         |         pinyin_city_area,pinyin_short_city_area,
         |         regexp_replace(regexp_replace(lower(ssid),'(\\\\s)',''),'([^a-z])','-') as ssid_en
         |  from $database.hotel_bssid_calculate_pinyin_base_info
         |  where day='$day'
         |) t
         |lateral view explode(split(ssid_en,'-')) b as ssid_split
         |where ssid_split != ''
       """.stripMargin)

    // 当用拼音全称来判断时，分段的ssid长度要>=5，酒店名字拼音必须包含分段的ssid，所在城市地区的拼音不能包含分段的ssid
    spark.sql(
      s"""
         |insert overwrite table $database.hotel_split_ssid_pinyin_match partition(day='$day')
         |select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         |       pinyin_name,pinyin_city_area,ssid_en,ssid_split
         |from $database.hotel_bssid_calculate_pinyin_ssid_split_base_info
         |where day='$day'
         |and instr(pinyin_name,ssid_split)>0
         |and instr(pinyin_city_area,ssid_split)=0
         |and length(regexp_replace(ssid_split,'^[zcs]hu\\B|[aeoi]ng$$','0'))>=5
       """.stripMargin)

    // 当用拼音缩写来判断时，所在城市地区的拼音不能包含分段的ssid
    // 分段的ssid长度要>=4并且酒店名字拼音必须包含分段的ssid，或者分段的ssid长度=3并且name_cut拼音必须包含分段的ssid
    spark.sql(
      s"""
         |insert overwrite table $database.hotel_split_ssid_pinyin_short_match partition(day='$day')
         |select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         |       pinyin_short_name,pinyin_cut_short_name,pinyin_short_city_area,ssid_en,ssid_split
         |from $database.hotel_bssid_calculate_pinyin_ssid_split_base_info
         |where day='$day'
         |and instr(pinyin_short_city_area,ssid_split)=0
         |and ((instr(pinyin_short_name,ssid_split)>0 and length(ssid_split)>=4)
         |  or (instr(pinyin_cut_short_name,ssid_split)>0 and length(ssid_split)=3))
       """.stripMargin)

    // ssid不分段
    // 根据拼音全称来判断，酒店名字的拼音长度>=4并且酒店名字的拼音是否在整段的ssid内，或者name_cut拼音长度>=4并且name_cut是否在整段的ssid内
    spark.sql(
      s"""
         |insert overwrite table $database.hotel_ssid_pinyin_match partition(day='$day')
         |select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         |       pinyin_name,pinyin_cut_name,ssid_en
         |from
         |(
         |  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         |         pinyin_name,pinyin_cut_name,
         |         regexp_replace(lower(ssid),'([^a-z])','') as ssid_en
         |  from $database.hotel_bssid_calculate_pinyin_base_info
         |  where day='$day'
         |)a
         |where (instr(ssid_en,pinyin_name)>0 and length(pinyin_name)>=4)
         |or (instr(ssid_en,pinyin_cut_name)>0 and length(pinyin_cut_name)>=4)
       """.stripMargin)

    // 根据拼音缩写来判断，酒店名字的拼音长度>=4并且酒店名字的拼音是否在整段的ssid内，或者name_cut拼音长度=3并且name_cut是否在整段的ssid内
    spark.sql(
      s"""
         |insert overwrite table $database.hotel_ssid_pinyin_short_match partition(day='$day')
         |select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         |       pinyin_short_name,pinyin_cut_short_name,ssid_en
         |from
         |(
         |  select name,poi_lat,poi_lon,city_code,ssid,device_num,connect_num,active_days,bssid_num,
         |         pinyin_short_name,pinyin_cut_short_name,
         |         regexp_replace(lower(ssid),'([^a-z])','') as ssid_en
         |  from $database.hotel_bssid_calculate_pinyin_base_info
         |  where day='$day'
         |)a
         |where (instr(ssid_en,pinyin_short_name)>0 and length(pinyin_short_name)>=4)
         |or (instr(ssid_en,pinyin_cut_short_name)>0 and length(pinyin_cut_short_name)=3)
       """.stripMargin)
  }
}
