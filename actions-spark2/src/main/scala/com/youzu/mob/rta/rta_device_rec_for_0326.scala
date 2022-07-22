package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object rta_device_rec_for_0326 {

  def dm_device_rec_for_0326_pre_table(spark: SparkSession, insert_day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0326_PRE
         | select
         |  code,
         |  idtype,
         |  idvalue,
         |  case when sum(appnum)=1 then 1
         |  when sum(appnum)=2 then 2
         |  when sum(appnum)=3 then 3
         |  when sum(appnum)=4 then 4
         |  when sum(appnum)>=5 then 5 end as recommend,1 as status
         |    from
         |     (
         |      select a.idtype,a.idvalue,a.appnum,concat('0326',b.code_id) as code
         |      from
         |        (
         |        select idtype,idvalue,appnum,cate_l2_clean
         |        from $DW_DEVICE_WITH_CATE_INSTALLCOUNT
         |        where day='$insert_day' and type=1
         |        ) a
         |    join
         |        (
         |        select game_cat,code_id from $RTA_GAMEAPP_CATE_40
         |        ) b
         |        on a.cate_l2_clean=b.game_cat
         |      ) c
         |      group by code,idtype,idvalue
         |      union
         |         select '032640' as code,'ieid' as idtype,ieid as idvalue,0.9 as recommend,1 as status from $ZHIHUI_GAME_DATA_IEID group by ieid
         |      union
         |         select '032640' as code,'oiid' as idtype,oiid as idvalue,0.9 as recommend,1 as status from $ZHIHUI_GAME_DATA_OIID group by oiid
         |""".stripMargin)
  }

  def dm_device_rec_for_0326_pre_second_table(spark: SparkSession, insert_day: String,full_par: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0326_PRE_SECOND
         |   select
         |   code,
         |   idtype,
         |   idvalue,
         |   recommend,
         |   status
         |     from $DM_DEVICE_REC_FOR_0326_PRE
         | union
         |   select
         |   a.code,
         |   a.idtype,
         |   a.idvalue,
         |   a.recommend,
         |   1 as status
         |     from
         |       (
         |         select code,idtype,idvalue,recommend
         |           from $DM_DEVICE_REC_FOR_0326
         |           where day='$full_par' and status!=0
         |        ) a
         |   left anti join $DM_DEVICE_REC_FOR_0326_PRE b
         |   on a.idtype=b.idtype and a.idvalue=b.idvalue
         |   left anti join
         |     (
         |       select idtype,idvalue
         |       from $DW_DEVICE_WITH_CATE_INSTALLCOUNT
         |       where day='$insert_day' group by idtype,idvalue
         |     ) c
         |       on a.idtype=c.idtype and a.idvalue=c.idvalue
         |   left anti join
         |     (
         |     select type as idtype,idvalue
         |     from $DW_GAASID_BLACKLIST where day='$insert_day'
         |     ) d on a.idtype=d.idtype and a.idvalue=d.idvalue
         |""".stripMargin
    )
  }

  def dm_device_rec_for_0326_table(spark: SparkSession, insert_day: String,full_par: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0326 partition(day='$insert_day')
         | select
         |   coalesce(a.code, b.code) as code,
         |   coalesce(a.idtype, b.idtype) as idtype,
         |   coalesce(a.idvalue, b.idvalue) as idvalue,
         |   coalesce(a.recommend, b.recommend) as recommend,
         |   case when a.recommend = b.recommend then 2 when a.recommend is not null
         |   and b.recommend is null then 1 when a.recommend is not null
         |   and b.recommend is not null
         |   and a.recommend != b.recommend then 1 else 0 end as status
         | from
         |   $DM_DEVICE_REC_FOR_0326_PRE_SECOND a
         |   full join
         |   (
         |     select code,idtype,idvalue,recommend
         |     from $DM_DEVICE_REC_FOR_0326
         |     where day = '$full_par'
         |   ) b
         |   on a.code = b.code and a.idtype = b.idtype and a.idvalue = b.idvalue
         |""".stripMargin
    )
  }


  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("rta_device_rec_for_0326")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val insert_day = args(0)
    val full_par = args(1)

    //新建临时表，包括本次所有0326推荐设备数据
    dm_device_rec_for_0326_pre_table(spark,insert_day)
    println("step13: dm_device_rec_for_0326_pre_table 执行完毕")

    //新建临时表，包括本次所有0326推荐设备数据+pre表中没有但在0326表次新分区中有，且未发回安装数据且不在黑名单的设备
    dm_device_rec_for_0326_pre_second_table(spark,insert_day,full_par)
    println("step14: dm_device_rec_for_0326_pre_second_table 执行完毕")

    //更新至0326规则结果表最新分区，增加增改、删、不变的状态
    dm_device_rec_for_0326_table(spark,insert_day,full_par)
    println("step15: dm_device_rec_for_0326_table 执行完毕")

    //关闭程序
    spark.stop()
  }
}
