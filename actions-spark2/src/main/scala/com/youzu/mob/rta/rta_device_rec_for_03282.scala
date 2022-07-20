package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object rta_device_rec_for_03282 {

  def dm_device_rec_for_03282_pre_table(spark: SparkSession,dim_app_pkg_par: String) = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_03282_PRE
         |select code,idtype,idvalue,recommend,status
         |from (
         |    select 'ieid'as idtype,idvalue,code_id as code,
         |    case when diffdays <=7 then '1'
         |         when diffdays <=14 then '2'
         |         when diffdays <30 then '3'
         |    end as recommend,1 as status
         |    from (
         |        select idvalue,b.code_id,min(a.diffdays) as diffdays
         |        from (
         |            select idvalue,coalesce(bb.apppkg,aa.apppkg) as apppkg,diffdays
         |            from (
         |                select idvalue,apppkg,
         |                cast(datediff(current_date,to_date(max(day),'yyyyMMdd')) as int) as diffdays
         |                from $DW_GAASID_IN_ACTIVEID
         |                where idtype='ieid'
         |                group by idvalue,apppkg
         |            ) aa left join (
         |                select pkg, apppkg
         |                from $DIM_APP_PKG_MAPPING_PAR
         |                where version='$dim_app_pkg_par'
         |                group by pkg,apppkg
         |            ) bb on aa.apppkg=bb.pkg
         |        )a
         |        inner join (
         |            select apppkg,concat('03282',code_id)as code_id
         |            from  $DW_APP_CATE_MAPPING aa
         |            inner join  (
         |              select * from $RTA_GAMEAPP_CATE_40
         |            )bb on aa.cate_l2=bb.game_cat
         |        )b
         |        on a.apppkg=b.apppkg
         |        group by idvalue,b.code_id
         |    )c
         |)d
         |where recommend is not null
         |union all
         |select code,idtype,idvalue,recommend,status
         |from (
         |    select 'oiid' as idtype,idvalue,code_id as code,
         |    case when diffdays <=7 then '1'
         |         when diffdays <=14 then '2'
         |         when diffdays <30 then '3'
         |    end as recommend,1 as status
         |    from (
         |        select idvalue,f.code_id,min(e.diffdays) as diffdays
         |        from (
         |             select idvalue,coalesce(bb.apppkg,aa.apppkg) as apppkg,diffdays
         |             from (
         |                 select idvalue,apppkg,
         |                     cast(datediff(current_date,to_date(max(day),'yyyyMMdd')) as int) as diffdays
         |                 from $DW_GAASID_IN_ACTIVEID
         |                 where idtype='oiid'
         |                 group by idvalue,apppkg
         |             ) aa left join (
         |                 select pkg, apppkg
         |                 from $DIM_APP_PKG_MAPPING_PAR
         |                 where version='$dim_app_pkg_par'
         |                 group by pkg,apppkg
         |             ) bb on aa.apppkg=bb.pkg
         |        )e
         |        inner join (
         |            select apppkg,concat('03282',code_id)as code_id
         |                from  $DW_APP_CATE_MAPPING aa
         |                inner join  (
         |                  select * from $RTA_GAMEAPP_CATE_40
         |                )bb on aa.cate_l2=bb.game_cat
         |        )f
         |        on e.apppkg=f.apppkg
         |        group by idvalue,f.code_id
         |    )g
         |)h
         |where recommend is not null
         |""".stripMargin)
  }

  def dm_device_rec_for_03282_table(spark: SparkSession, insert_day: String, full_par: String) = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_03282 partition(day='$insert_day')
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
         |     $DM_DEVICE_REC_FOR_03282_PRE a
         | full join
         |  (
         |    select code,idtype,idvalue,recommend
         |    from $DM_DEVICE_REC_FOR_03282
         |    where day = '$full_par'
         |  ) b
         |   on a.code = b.code and a.idtype = b.idtype and a.idvalue = b.idvalue
         |""".stripMargin
    )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("rta_device_rec_for_03282")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //传入日期参数
    val insert_day = args(0)
    val full_par = args(1)
    val dim_app_pkg_par=args(2)
    //新建03282临时结果表，根据最近活跃时间构建人群包
    dm_device_rec_for_03282_pre_table(spark,dim_app_pkg_par)
    println("dm_device_rec_for_03282_pre_table")

    //更新至03282规则结果表最新分区，增加增改、删、不变的状态
    dm_device_rec_for_03282_table(spark, insert_day, full_par)
    println("dm_device_rec_for_03282_table")

    //关闭程序
    spark.stop()
  }
}
