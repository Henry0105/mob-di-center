package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object rta_device_rec_for_0325 {

  def dm_device_rec_for_0325_pre_table(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0325_PRE
         | select
         | '03251' as code,
         | 'ieid' as idtype,
         | id as idvalue,
         | recommend,
         | 1 as stauts
         |   from
         |   (
         |    select
         |      a.id, case when cnt is null then 1 when cnt=1 then 1.01 when cnt=2 then 1.02 when cnt=3 then 1.03
         |        when cnt=4 then 1.04 when cnt=5 then 1.05 when cnt=6 then 1.06 when cnt=7 then 1.07
         |        when cnt=8 then 1.08 when cnt=9 then 1.09 when cnt=10 then 1.1 when cnt=11 then 1.11
         |        when cnt=12 then 1.12 when cnt=13 then 1.13 when cnt=14 then 1.14
         |        end as recommend
         |         from
         |         (
         |           select distinct(id)
         |            from $ODS_DPI_MKT_FEEDBACK_INCR
         |            where load_day>='20220328' and source='guangdong_mobile' and model_type='timewindow' and tag!='mb11') a
         |            left join
         |             (
         |              select a.id, count(1) as cnt
         |               from
         |                (
         |                 select id
         |                  from $ODS_DPI_MKT_FEEDBACK_INCR
         |                  where datediff(current_date,date(concat(substr(day,1,4),'-',substr(day,5,2),'-',substr(day,-2))))<=14
         |                      and source='guangdong_mobile' and model_type='timewindow' and tag!='mb11'
         |                  group by id,day
         |                ) a
         |             group by id
         |             ) b
         |             on a.id=b.id
         |          ) c
         | union
         |    select
         |    '03251' as code,
         |    'oiid' as idtype,
         |    d.oiid as idvalue,
         |    recommend,
         |    1 as status
         |    from
         |      (
         |        select
         |           a.id, case when cnt is null then 1 when cnt=1 then 1.01 when cnt=2 then 1.02 when cnt=3 then 1.03
         |             when cnt=4 then 1.04 when cnt=5 then 1.05 when cnt=6 then 1.06 when cnt=7 then 1.07
         |             when cnt=8 then 1.08 when cnt=9 then 1.09 when cnt=10 then 1.1 when cnt=11 then 1.11
         |             when cnt=12 then 1.12 when cnt=13 then 1.13 when cnt=14 then 1.14
         |             end as recommend
         |          from
         |            (
         |             select distinct(id)
         |               from $ODS_DPI_MKT_FEEDBACK_INCR
         |               where  load_day>='20220328' and source='guangdong_mobile' and model_type='timewindow' and tag!='mb11') a
         |             left join
         |              (
         |               select a.id, count(1) as cnt
         |               from
         |                 (
         |                 select id
         |                 from $ODS_DPI_MKT_FEEDBACK_INCR
         |                 where datediff(current_date,date(concat(substr(day,1,4),'-',substr(day,5,2),'-',substr(day,-2))))<=14
         |                     and source='guangdong_mobile' and model_type='timewindow' and tag!='mb11'
         |                 group by id,day
         |                 ) a
         |             group by id
         |              ) b on a.id=b.id
         |         ) c
         |          join  $DPI_IEID_OIID_20220329 d
         |         on c.id=d.ieid
         | union
         |     select
         |      '03252' as code,
         |      'ieid' as idtype,
         |      a.id as idvalue,
         |      case when diffdays<=7 then 0.9 when diffdays>7 and diffdays<=14 then 1 when diffdays>14 then 1.1 end as recommend,
         |      1 as status
         |     from
         |        (
         |          select id, cast(datediff(current_date,date(concat(substr(max(day),1,4),'-',substr(max(day),5,2),'-',substr(max(day),-2)))) as int) as diffdays
         |          from $ODS_DPI_MKT_FEEDBACK_INCR
         |          where load_day>='20220328' and source='guangdong_mobile' and model_type='timewindow' and tag!='mb11'
         |          group by id
         |        ) a
         | union
         |     select
         |      '03252' as code,
         |      'oiid' as idtype,
         |      b.oiid as idvalue,
         |      case when diffdays<=7 then 0.9 when diffdays>7 and diffdays<=14 then 1 when diffdays>14 then 1.1 end as recommend,
         |      1 as status
         |     from
         |       (
         |        select
         |        id,
         |        cast(datediff(current_date,date(concat(substr(max(day),1,4),'-',substr(max(day),5,2),'-',substr(max(day),-2)))) as int) as diffdays
         |        from $ODS_DPI_MKT_FEEDBACK_INCR
         |        where load_day>='20220328' and source='guangdong_mobile' and model_type='timewindow' and tag!='mb11'
         |        group by id
         |       ) a
         |        join  $DPI_IEID_OIID_20220329 b
         |       on a.id=b.ieid
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
