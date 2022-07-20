package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object rta_device_rec_for_0324 {

  def dm_device_rec_for_0324_pre_table(spark: SparkSession, insert_day: String) = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0324_PRE
         |select
         |    '0324' as code,
         |    idtype,
         |    idvalue,
         |    0.8 as recommend,
         |    1 as status
         |from $DW_DEVICE_WITH_CATE_INSTALLCOUNT
         |where day='$insert_day' and type=1
         |group by idtype,idvalue
         |union
         |select
         |    '0324' as code,
         |    a.idtype,
         |    a.idvalue,
         |    0.1 as recommend,
         |    1 as status
         |from
         |      (
         |         select idtype, idvalue
         |         from $DW_DEVICE_WITH_CATE_INSTALLCOUNT
         |         where day='$insert_day' and type!=1
         |         group by idtype,idvalue
         |     ) a
         |     left join
         |     (
         |         select idtype, idvalue
         |         from $DW_DEVICE_WITH_CATE_INSTALLCOUNT
         |         where day='$insert_day' and type=1
         |         group by idtype,idvalue
         |     ) b
         |     on a.idtype=b.idtype and a.idvalue=b.idvalue
         |     where b.idvalue is null
         |""".stripMargin
    )
  }

  def dm_device_rec_for_0324_pre_second_table(spark: SparkSession, insert_day: String, full_par: String) = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0324_PRE_SECOND
         |  select
         |  code,
         |  idtype,
         |  idvalue,
         |  recommend,
         |  status
         |from $DM_DEVICE_REC_FOR_0324_PRE
         |union
         |select
         |   a.code,
         |   a.idtype,
         |   a.idvalue,
         |   a.recommend,
         |   1 as status
         |    from
         |      (
         |       select code,idtype,idvalue,recommend
         |       from $DM_DEVICE_REC_FOR_0324
         |       where day='$full_par' and status!=0
         |      ) a
         |       left anti join $DM_DEVICE_REC_FOR_0324_PRE b
         |       on a.idtype=b.idtype and a.idvalue=b.idvalue
         |     left anti join
         |      (
         |        select idtype,idvalue
         |        from $DW_DEVICE_WITH_CATE_INSTALLCOUNT
         |        where day='$insert_day' group by idtype,idvalue
         |      ) c
         |      on a.idtype=c.idtype and a.idvalue=c.idvalue
         |     left anti join
         |      (
         |        select type as idtype,idvalue
         |        from $DW_GAASID_BLACKLIST where day='$insert_day'
         |      ) d
         |       on a.idtype=d.idtype and a.idvalue=d.idvalue
         |""".stripMargin
    )
  }

  def dm_device_rec_for_0324_table(spark: SparkSession, insert_day: String, full_par: String) = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0324 partition(day='$insert_day')
         |  select
         |   coalesce(a.code, b.code) as code,
         |   coalesce(a.idtype, b.idtype) as idtype,
         |   coalesce(a.idvalue, b.idvalue) as idvalue,
         |   coalesce(a.recommend, b.recommend) as recommend,
         |   case when a.recommend = b.recommend then 2 when a.recommend is not null
         |     and b.recommend is null then 1 when a.recommend is not null
         |     and b.recommend is not null
         |     and a.recommend != b.recommend then 1 else 0 end as status
         | from
         |   $DM_DEVICE_REC_FOR_0324_PRE_SECOND a
         |   full join
         |   (
         |     select code,idtype,idvalue,recommend
         |     from $DM_DEVICE_REC_FOR_0324
         |     where day = '$full_par'
         |   ) b on a.code = b.code and a.idtype = b.idtype and a.idvalue = b.idvalue
         |""".stripMargin
    )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("rta_device_rec_for_0324")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val insert_day = args(0)
    val full_par = args(1)


    //规则数据生成_redis应用
    //写入0324规则数据
    //新建临时表，包括本次所有0324推荐设备数据
    dm_device_rec_for_0324_pre_table(spark,insert_day)
    println("step8: dm_device_rec_for_0324_pre_table 执行完毕")

    //新建临时表，包括本次所有0324推荐设备数据+pre表中没有但在0324表次新分区中有，且未发回安装数据且不在黑名单的设备
    dm_device_rec_for_0324_pre_second_table(spark,insert_day, full_par)
    println("step9: dm_device_rec_for_0324_pre_second_table 执行完毕")

    //更新至0324规则结果表最新分区，增加增改、删、不变的状态
    dm_device_rec_for_0324_table(spark,insert_day,full_par)
    println("step10: dm_device_rec_for_0324_table 执行完毕")

    //关闭程序
    spark.stop()
  }
}
