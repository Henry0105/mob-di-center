package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object rta_device_rec_for_03281 {

  def dm_device_rec_for_03281_pre_table(spark: SparkSession,dim_app_pkg_par: String) = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_03281_PRE
         |select code,idtype,idvalue,
         |case when recommend <=5 then '1'
         |     when recommend <=10 then '2'
         |     when recommend <=15 then '3'
         |     when recommend <=20 then '4'
         |     when recommend <=25 then '5'
         |     when recommend <=30 then '6'
         |     when recommend <=35 then '7'
         |     when recommend <=40 then '8'
         |     when recommend <=45 then '9'
         |     when recommend <=50 then '10'
         |     when recommend <=55 then '11'
         |     when recommend <=60 then '12'
         |     when recommend >=61 then '13'
         |end as recommend,1 as status
         |from (
         |      select idtype,idvalue,code_id as code,count(1) as recommend
         |from (
         |    select /*+ BROADCASTJOIN(b) */ a.idtype,a.idvalue,a.apppkg,a.day,b.code_id
         |    from (
         |        select idtype,idvalue,coalesce(bb.apppkg,aa.apppkg) as apppkg,day
         |        from (
         |            select *
         |            from $DW_GAASID_IN_ACTIVEID
         |            where idtype='ieid'
         |        ) aa left join (
         |            select pkg, apppkg
         |            from $DIM_APP_PKG_MAPPING_PAR
         |            where version='$dim_app_pkg_par'
         |            group by pkg,apppkg
         |        ) bb on aa.apppkg=bb.pkg
         |    )a inner join (
         |        select apppkg,concat('03281',code_id)as code_id
         |        from  $DW_APP_CATE_MAPPING aa
         |        inner join  (
         |          select * from $RTA_GAMEAPP_CATE_40
         |        )bb on aa.cate_l2=bb.game_cat
         |    )b --39个codeid
         |    on a.apppkg=b.apppkg
         |)c
         |group by idtype,idvalue,code_id
         |union
         |select idtype,idvalue,code_id as code,count(1) as recommend
         |from (
         |    select /*+ BROADCASTJOIN(b) */ a.idtype,a.idvalue,a.apppkg,a.day,b.code_id
         |    from (
         |        select idtype,idvalue,coalesce(bb.apppkg,aa.apppkg) as apppkg,day
         |        from (
         |            select *
         |            from $DW_GAASID_IN_ACTIVEID
         |            where idtype='oiid'
         |        ) aa left join (
         |            select pkg, apppkg
         |            from $DIM_APP_PKG_MAPPING_PAR
         |            where version='$dim_app_pkg_par'
         |            group by pkg,apppkg
         |        ) bb on aa.apppkg=bb.pkg
         |    )a inner join (
         |        select apppkg,concat('03281',code_id)as code_id
         |        from  $DW_APP_CATE_MAPPING aa
         |        inner join  (
         |          select * from $RTA_GAMEAPP_CATE_40
         |        )bb on aa.cate_l2=bb.game_cat
         |    )b --39个codeid
         |    on a.apppkg=b.apppkg
         |)c
         |group by idtype,idvalue,code_id
         |)d
        """.stripMargin
    )
  }

  def dm_device_rec_for_03281_table(spark: SparkSession, insert_day: String, full_par: String) = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_03281 partition(day='$insert_day')
         |   select
         |     coalesce(a.code, b.code) as code,
         |     coalesce(a.idtype, b.idtype) as idtype,
         |     coalesce(a.idvalue, b.idvalue) as idvalue,
         |     coalesce(a.recommend, b.recommend) as recommend,
         |     case when a.recommend = b.recommend then 2
         |          when a.recommend is not null
         |               and b.recommend is null then 1
         |          when a.recommend is not null
         |               and b.recommend is not null
         |               and a.recommend != b.recommend then 1
         |          else 0
         |     end as status
         |   from
         |     $DM_DEVICE_REC_FOR_03281_PRE a
         | full join
         |  (
         |    select code,idtype,idvalue,recommend
         |    from $DM_DEVICE_REC_FOR_03281
         |    where day = '$full_par'
         |  ) b
         |   on a.code = b.code and a.idtype = b.idtype and a.idvalue = b.idvalue
         |""".stripMargin
    )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("rta_device_rec_for_03281")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //传入日期参数
    val insert_day = args(0)
    val full_par = args(1)
    val dim_app_pkg_par = args(2)

    //新建0328xx_1临时结果表，根据月活天数构建人群包
    dm_device_rec_for_03281_pre_table(spark,dim_app_pkg_par)
    println("dm_device_rec_for_03281_pre_table")

    //更新至0328xx_1规则结果表最新分区，增加增改、删、不变的状态
    dm_device_rec_for_03281_table(spark, insert_day, full_par)
    println("dm_device_rec_for_03281_table")

    //关闭程序
    spark.stop()
  }
}
