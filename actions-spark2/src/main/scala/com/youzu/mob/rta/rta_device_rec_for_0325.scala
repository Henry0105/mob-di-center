package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object rta_device_rec_for_0325 {
  def dpi_feedback_ieid_table(spark: SparkSession):Unit={
    spark.sql(
      s"""
         |insert overwrite table $DPI_FEEDBACK_IEID
         |select id,day
         |from $ODS_DPI_MKT_FEEDBACK_INCR
         |where load_day>='20220328' and source='guangdong_mobile' and model_type='timewindow' and tag!='mb11'
         |group by id,day
         |union
         |select b.ieid as id ,day
         |from(
         |    select id,day
         |    from $ODS_DPI_MKT_FEEDBACK_INCR
         |    where load_day>='20220604' and source='unicom' and model_type='timewindow' and tag!='mb11'
         |    group by id,day
         |)a
         |inner join
         |$RTA_DIP_CUCC_IEID_WITH_RN b
         |on a.id=b.id
        |""".stripMargin)
  }

  def dm_device_rec_for_0325_pre_table(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0325_PRE
         |select '03251' as code,'ieid' as idtype,a.id as idvalue,
         |    case when cnt =0 then 1 when cnt=1 then 1.01 when cnt=2 then 1.02 when cnt=3 then 1.03
         |    when cnt=4 then 1.04 when cnt=5 then 1.05 when cnt=6 then 1.06 when cnt=7 then 1.07
         |    when cnt=8 then 1.08 when cnt=9 then 1.09 when cnt=10 then 1.1 when cnt=11 then 1.11
         |    when cnt=12 then 1.12 when cnt=13 then 1.13 when cnt=14 then 1.14
         |    end as recommend,
         |    1 as stauts
         |from
         |(
         |   select id,sum(if(datediff(current_date,to_date(day,'yyyyMMdd'))<=14,1,0)) as cnt
         |   from $DPI_FEEDBACK_IEID
         |   group by id
         |) a
         |union
         |select '03251' as code,'oiid' as idtype,oiid as idvalue,
         |      case when cnt=0 then 1 when cnt=1 then 1.01 when cnt=2 then 1.02 when cnt=3 then 1.03
         |      when cnt=4 then 1.04 when cnt=5 then 1.05 when cnt=6 then 1.06 when cnt=7 then 1.07
         |      when cnt=8 then 1.08 when cnt=9 then 1.09 when cnt=10 then 1.1 when cnt=11 then 1.11
         |      when cnt=12 then 1.12 when cnt=13 then 1.13 when cnt=14 then 1.14
         |      end as recommend,1 as stauts
         |from
         |(
         |   select oiid,sum(if(datediff(current_date,to_date(day,'yyyyMMdd'))<=14,1,0)) as cnt
         |   from (
         |   select b.oiid,a.day
         |   from $DPI_FEEDBACK_IEID a
         |   inner join
         |   $DW_GAAS_ID_DATA_DI_RTA b
         |   on a.id=b.ieid
         |   group by b.oiid,a.day
         |   )d
         |   group by oiid
         |) c
         |union
         |select '03252' as code, 'ieid' as idtype, a.id as idvalue,
         |      case when diffdays<=7 then 0.9 when diffdays>7 and diffdays<=14 then 1
         |           when diffdays>14 then 1.1
         |      end as recommend,1 as status
         |from
         |(
         |  select id, cast(datediff(current_date,to_date(max(day),'yyyyMMdd')) as int) as diffdays
         |  from $DPI_FEEDBACK_IEID
         |  group by id
         |) a
         |union
         |select '03252' as code, 'oiid' as idtype, oiid as idvalue,
         |      case when diffdays<=7 then 0.9 when diffdays>7 and diffdays<=14 then 1
         |           when diffdays>14 then 1.1
         |      end as recommend,1 as status
         |from
         |(
         | select oiid, cast(datediff(current_date,to_date(max(day),'yyyyMMdd')) as int) as diffdays
         |  from (
         |  select b.oiid,a.day
         |   from $DPI_FEEDBACK_IEID a
         |   inner join
         |   $DW_GAAS_ID_DATA_DI_RTA b
         |   on a.id=b.ieid
         |   group by b.oiid,a.day
         |  )d
         |  group by oiid
         |) c
         |""".stripMargin)
  }

  def dm_device_rec_for_0325_table(spark: SparkSession, insert_day: String, full_par: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0325 partition(day='$insert_day')
         |  select
         |    coalesce(a.code, b.code) as code,
         |    coalesce(a.idtype, b.idtype) as idtype,
         |    coalesce(a.idvalue, b.idvalue) as idvalue,
         |    coalesce(a.recommend, b.recommend) as recommend,
         |    case when a.recommend = b.recommend then 2 when a.recommend is not null
         |    and b.recommend is null then 1 when a.recommend is not null
         |    and b.recommend is not null
         |    and a.recommend != b.recommend then 1 else 0 end as status
         |  from
         |    $DM_DEVICE_REC_FOR_0325_PRE a
         |    full join
         |    (
         |      select code,idtype,idvalue,recommend
         |      from $DM_DEVICE_REC_FOR_0325
         |      where day = '$full_par'
         |    ) b
         |    on a.code = b.code and a.idtype = b.idtype and a.idvalue = b.idvalue
         |""".stripMargin
    )
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("rta_device_rec_for_0325")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val insert_day = args(0)
    val full_par = args(1)

    //新建联通和移动的ieid临时表，仅包含付费时间维度
    dpi_feedback_ieid_table(spark)

    //新建0325临时结果表，包括付费天数及距今付费间隔时间维度的设备
    dm_device_rec_for_0325_pre_table(spark)
    println("step11: dm_device_rec_for_0325_pre_table 执行完毕")

    //更新至0325规则结果表最新分区，增加增改、删、不变的状态
    dm_device_rec_for_0325_table(spark, insert_day, full_par)
    println("step12: dm_device_rec_for_0325_table 执行完毕")

    //关闭程序
    spark.stop()
  }
}
