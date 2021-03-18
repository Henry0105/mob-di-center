package com.youzu.mob.stall

import org.apache.spark.sql.SparkSession
import com.youzu.mob.utils.Constants._

class StallDataWare {

  /**
    * sourcetable dw_mobdi_etl.log_device_install_app_incr_info dw_mobdi_etl.log_device_unstall_app_info
    * targettable dm_mobdi_master.dwd_device_app_install_action_di
    *
    * @param spark
    * @param datetime
    */
  def appInstallActionDI(spark: SparkSession, datetime: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DWS_DEVICE_APP_INSTALL_DI partition(day=${datetime})
         |select device,pkg,
         |       max(install_flag) as install_flag,
         |       max(unstall_flag) as unstall_flag,
         |       max(install_datetime) as install_datetime,
         |       max(unstall_datetime) as unstall_datetime,
         |       case when max(install_datetime)>max(unstall_datetime) then 1 else -1 end as final_flag,
         |       case when max(install_datetime)>max(unstall_datetime) then max(install_datetime) else max(unstall_datetime) end as final_time,
         |       concat_ws(',',sort_array(collect_list(trace))) as trace_list
         |from
         |(
         |  select trim(lower(deviceid)) as device,
         |         trim(pkg) as pkg,
         |         0 as install_flag,
         |         1 as unstall_flag,
         |         0 as install_datetime,
         |         if(length(cast(clienttime as string))=13,
         |           clienttime,
         |           concat(unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss'), '000')) as unstall_datetime,
         |         concat_ws('=',
         |           if(length(cast(clienttime as string))=13,
         |             clienttime,
         |             concat(unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss'), '000')),
         |           '-1') as trace
         |  from $DWD_LOG_DEVICE_UNSTALL_APP_INFO_SEC_DI
         |  where day='${datetime}'
         |  and trim(lower(deviceid)) rlike '^[a-f0-9]{40}$$'
         |  and trim(deviceid)!='0000000000000000000000000000000000000000'
         |  and pkg is not null
         |  and trim(pkg) not in ('','null','NULL')
         |  and trim(pkg)=regexp_extract(trim(pkg),'([a-zA-Z0-9\\.\\_-]+)',0)
         |
         |  union all
         |
         |  select trim(lower(device)) as device,
         |         trim(pkg) as pkg,
         |         1 as install_flag,
         |         0 as unstall_flag,
         |         if(length(cast(clienttime as string))=13,
         |           clienttime,
         |           concat(unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss'), '000')) as install_datetime,
         |         0 as unstall_datetime,
         |         concat_ws('=',
         |           if(length(cast(clienttime as string))=13,
         |             clienttime,
         |             concat(unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss'), '000')),
         |           '1') as trace
         |  from $DWD_LOG_DEVICE_INSTALL_APP_INCR_INFO_SEC_DI
         |  where day='${datetime}'
         |  and trim(lower(device)) rlike '^[a-f0-9]{40}$$'
         |  and trim(device)!='0000000000000000000000000000000000000000'
         |  and pkg is not null
         |  and trim(pkg) not in ('','null','NULL')
         |  and trim(pkg)=regexp_extract(trim(pkg),'([a-zA-Z0-9\\.\\_-]+)',0)
         |)a
         |group by device,pkg
      """.stripMargin)
  }

  def appInstallActionDF(spark: SparkSession, datetime: String): Unit = {

    spark.sql(
      s"""
         |insert overwrite table $DWS_DEVICE_APP_INFO_DF partition(day=${datetime})
         |select device,pkg,max(all_time) as all_time
         |from
         |(
         |  select device,pkg,
         |         if(length(cast(clienttime as string))=13,
         |           clienttime,
         |           concat(unix_timestamp(serdatetime, 'yyyy-MM-dd HH:mm:ss'), '000')) as all_time
         |  from
         |  (
         |    select trim(lower(device)) as device,
         |           trim(pkg) as pkg,
         |           serdatetime,
         |           clienttime,
         |           rank() over(partition by trim(lower(device)) order by serdatetime desc ) as rank
         |    from $DWD_LOG_DEVICE_INSTALL_APP_ALL_INFO_SEC_DI
         |    where day=${datetime}
         |    and trim(lower(device)) rlike '^[a-f0-9]{40}$$'
         |    and trim(device)!='0000000000000000000000000000000000000000'
         |    and pkg is not null
         |    and trim(pkg) not in ('','null','NULL')
         |    and trim(pkg)=regexp_extract(trim(pkg),'([a-zA-Z0-9\\.\\_-]+)',0)
         |  ) aa
         |  where rank=1
         |) t1
         |group by device,pkg
      """.stripMargin)
  }
}

object StallDataWare {

  def main(args: Array[String]): Unit = {

    val stall = new StallDataWare

    val datetime = args(0)
    val tableflag = args(1)

    val spark = initial(s"stallDataWare  ${tableflag}")

    tableflag.trim match {
      case "appInstallDI" => stall.appInstallActionDI(spark, datetime)
      case "appInstallDF" => stall.appInstallActionDF(spark, datetime)
    }

    spark.stop()
  }
}
