package com.youzu.mob.stall

import org.apache.spark.sql.SparkSession


class StallDataWareService {

  /**
    * sourcetable   ${db}.dwd_device_app_info_df,${db}.dwd_device_app_install_di
    * targettable ${db}.dws_device_install_status
    *
    * @param spark
    * @param datetime
    */
  def statusStall(spark: SparkSession, datetime: String, statustime: String, p180day: String,
                  db: String = "dm_mobdi_master"): Unit = {

    val partTable1 = "rp_mobdi_app.tmp_install_status_part1_" + System.currentTimeMillis()
    val partTable2 = "rp_mobdi_app.tmp_install_status_part2_" + System.currentTimeMillis()
    val partTable3 = "rp_mobdi_app.tmp_install_status_part3_" + System.currentTimeMillis()
    val partTable4 = "rp_mobdi_app.tmp_install_status_part4_" + System.currentTimeMillis()
    val partTable5 = "rp_mobdi_app.tmp_install_status_part5_" + System.currentTimeMillis()
    val partTable6 = "rp_mobdi_app.tmp_install_status_part6_" + System.currentTimeMillis()

    // full表中保存的当日在装设备的信息
    spark.sql(
      s"""
         |create table if not exists ${partTable1} as
         |select t1.device,pkg,final_flag,final_time,process_time,
         |       install_datetime,unstall_datetime,all_datetime,reserved_flag
         |from
         |(
         |  select *
         |  from ${db}.dws_device_install_status
         |  where day='${statustime}' and reserved_flag <> '-1'
         |) t1
         |inner join
         |(
         |  select device
         |  from ${db}.dwd_device_app_info_df
         |  where day='${datetime}'
         |  group by device
         |) part on part.device = t1.device
      """.stripMargin)

    spark.sql(
      s"""
         |create table if not exists ${partTable2} as
         |select nvl(b.device,a.device) as device,
         |         nvl(b.pkg,a.pkg) as pkg,
         |         all_time as final_time,
         |         case when a.pkg is null then '-1'
         |              else '0'  end as final_flag,
         |         case when a.pkg is null then '-1'
         |              when b.pkg is null then '1'
         |              else '0' end as reserved_flag,
         |         install_datetime,unstall_datetime,
         |         case when nvl(a.all_time,'0')>nvl(b.all_datetime,'0') then nvl(a.all_time,'0')
         |              else nvl(b.all_datetime,'0') end as all_datetime
         |from
         |(
         |  select *
         |  from ${db}.dwd_device_app_info_df
         |  where day='${datetime}'
         |) a
         |full join ${partTable1} b
         |on b.device = a.device and b.pkg=a.pkg
      """.stripMargin)

    // df 对full表更新的device,pkg
    spark.sql(
      s"""
         |create table if not exists ${partTable3} as
         |select nvl(t1.device,part.device) as device,
         |       nvl(t1.pkg,part.pkg) as pkg,
         |       case when part.final_time is not null then part.final_time
         |            else t1.final_time end as final_time,
         |       case when part.final_flag is not null then part.final_flag
         |            else t1.final_flag end as final_flag,
         |       case when part.reserved_flag is not null then part.reserved_flag
         |            else '0' end as reserved_flag,
         |       case when nvl(t1.install_datetime,'0')>nvl(part.install_datetime,'0') then nvl(t1.install_datetime,'0')
         |       else nvl(part.install_datetime,'0') end as install_datetime,
         |       case when nvl(t1.unstall_datetime,'0')>nvl(part.unstall_datetime,'0') then nvl(t1.unstall_datetime,'0')
         |       else nvl(part.unstall_datetime,'0') end as unstall_datetime,
         |       case when nvl(t1.all_datetime,'0')>nvl(part.all_datetime,'0') then nvl(t1.all_datetime,'0')
         |       else nvl(part.all_datetime,'0') end as all_datetime,
         |       case when part.reserved_flag is not null then '${datetime}'
         |            else t1.process_time end as process_time
         |from
         |(
         |  select device,pkg,final_time,final_flag,process_time,install_datetime,unstall_datetime,all_datetime
         |  from ${db}.dws_device_install_status
         |  where day = '${statustime}'
         |) t1
         |full join ${partTable2} part
         |on part.device = t1.device and part.pkg=t1.pkg
      """.stripMargin)

    spark.sql(
      s"""
         |create table if not exists ${partTable4} as
         |select t1.device,pkg,trace_list,install_datetime,unstall_datetime
         |    from
         |    (select device,pkg,trace_list,install_datetime,unstall_datetime
         |     from ${db}.dwd_device_app_install_di
         |     where day='${datetime}'
         |    ) t1
         |    inner join
         |    (
         |      select device
         |      from ${partTable3}
         |      group by device
         |    ) t2
         |    on t1.device=t2.device
      """.stripMargin)

    spark.sql(
      s"""
         |create table if not exists ${partTable5} as
         |select device
         |  from
         |  (
         |    select device
         |    from ${db}.dwd_device_app_install_di
         |    where day='${datetime}'
         |
         |    union all
         |
         |    select device
         |    from ${db}.dwd_device_app_info_df
         |    where day='${datetime}'
         |  ) t
         |  group by device
      """.stripMargin)


    spark.sql(
      s"""
         |create table if not exists ${partTable6} as
         |select nvl(tt.device,incr.device) as device,
         |         nvl(tt.pkg,incr.pkg) as pkg,
         |         case when nvl(incr.install_datetime,'0')>nvl(tt.install_datetime,'0') then nvl(incr.install_datetime,'0')
         |              else nvl(tt.install_datetime,'0') end as install_datetime,
         |         case when nvl(incr.unstall_datetime,'0')>nvl(tt.unstall_datetime,'0') then incr.unstall_datetime
         |              else nvl(tt.unstall_datetime,'0') end as unstall_datetime,
         |         nvl(tt.all_datetime,'0') as all_datetime,
         |         case when incr.device is null then tt.final_flag
         |              else split(sort_array(split(concat_ws(',',concat_ws('=',tt.final_time,tt.final_flag),trace_list),','))
         |                 [size(split(trace_list,','))],'=')[1] end as final_flag,
         |         case when incr.device is null then tt.final_time
         |              else split(sort_array(split(concat_ws(',',concat_ws('=',tt.final_time,tt.final_flag),trace_list),','))
         |                [size(split(trace_list,','))],'=')[0] end as final_time,
         |         case when incr.device is null then tt.reserved_flag
         |              when tt.final_flag is null then split(sort_array(split(concat_ws(',',concat_ws('=',tt.final_time,tt.final_flag),trace_list),','))
         |                 [size(split(trace_list,','))],'=')[1]
         |              else countSum(concat_ws(',',sort_array(split(concat_ws(',',concat_ws('=',tt.final_time,tt.final_flag),trace_list),',')))) end as reserved_flag,
         |         nvl(tt.process_time,'${datetime}') as process_time
         |from ${partTable3} tt
         |full join ${partTable4} incr
         |on tt.device=incr.device and tt.pkg=incr.pkg
      """.stripMargin)


    // di数据对full 数据进行更新
    spark.sql(
      s"""
         |insert overwrite table ${db}.dws_device_install_status partition(day='${datetime}')
         |select tt.device,pkg,install_datetime,unstall_datetime,all_datetime,final_flag,final_time,reserved_flag,
         |       if(time.device is not null,'${datetime}',tt.process_time) as process_time
         |from ${partTable6} tt
         |left join ${partTable5} time
         |on tt.device=time.device
         |where if(time.device is not null,'${datetime}',tt.process_time)>'${p180day}'
      """.stripMargin)

    spark.sql(s"drop table if exists ${partTable1}")
    spark.sql(s"drop table if exists ${partTable2}")
    spark.sql(s"drop table if exists ${partTable3}")
    spark.sql(s"drop table if exists ${partTable4}")
    spark.sql(s"drop table if exists ${partTable5}")
    spark.sql(s"drop table if exists ${partTable6}")


  }

}

object StallDataWareService {

  def main(args: Array[String]): Unit = {

    val stall = new StallDataWareService
    // 输入参数
    val datetime = args(0)
    val statustime = args(1)
    val p180day = args(2)
    val tableFlag = args(3)
    val logLevel = if ("debug".equals(tableFlag)) "DEBUG" else "ERROR"


    // 初始化spark
    val spark = initial("stallDataWareService", logLevel)

    //
    tableFlag.trim match {
      case "statusStall" => stall.statusStall(spark, datetime, statustime, p180day, "dm_mobdi_master")
      case "test" | "debug" => stall.statusStall(spark, datetime, statustime, p180day, "mobdi_test")

    }

    spark.stop()
  }
}
