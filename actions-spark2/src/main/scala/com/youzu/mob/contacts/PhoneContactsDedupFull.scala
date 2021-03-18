package com.youzu.mob.contacts

import com.youzu.mob.utils.Constants._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PhoneContactsDedupFull {
  Logger.getLogger("org").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    if (args.length != 6) {
      println("parameter error")
      sys.exit(-1)
    }

    val contacts_start_day = args(0) // 抽取通讯录的起始天分区
    val contacts_end_day = args(1) // 抽取通讯录的结束天分区
    val last_snapshot_day = args(2) // 上一个快照的天分区
    val last_snapshot_hour = args(3) // 上一个快照的小时分区
    val current_snapshot_day = args(4) // 当前插入快照的天分区
    val current_snapshot_hour = args(5) // 当前插入快照的小时分区

    val main_sql =
      s"""
         |WITH id_mapping_dedup AS (--android_id_mapping表针对device取唯一（取processtime最近的一条，processtime相等时，取device值最大的一条。）
         |  SELECT id_mapping.device,
         |         id_mapping.duid,  --关联字段
         |         id_mapping.plat
         |  FROM
         |  (
         |    SELECT duid_mapping.device,
         |           duid_mapping.duid,
         |           duid_mapping.plat,
         |           row_number() over
         |           (PARTITION BY duid_mapping.duid
         |           ORDER BY duid_mapping.processtime DESC, duid_mapping.device DESC) rank
         |    FROM $DWS_DEVICE_DUID_MAPPING_NEW duid_mapping
         |    LEFT SEMI JOIN
         |    (  --优化降低join的duid数量
         |      SELECT phone_contacts.duid
         |      FROM $DM_MOBDI_TMP.phone_contacts phone_contacts
         |      WHERE phone_contacts.dt BETWEEN '${contacts_start_day}' AND '${contacts_end_day}'
         |      AND LENGTH(TRIM(phone_contacts.duid)) > 0
         |      GROUP BY phone_contacts.duid
         |    ) phone_contacts ON (duid_mapping.duid=phone_contacts.duid)
         |    WHERE LENGTH(TRIM(duid_mapping.device)) > 0
         |    AND LENGTH(TRIM(duid_mapping.duid)) > 0
         |    AND processtime<='${contacts_end_day}'
         |  ) id_mapping
         |  WHERE id_mapping.rank=1
         |),
         |
         |contacts_updated_dedup AS (  --从phone_contacts中取出新上传的用户通讯录数据(周期:7天)去重
         |  SELECT NVL(contacts_updated.device, '') AS device,
         |         NVL(contacts_updated.my_phone, '') AS my_phone,
         |         NVL(contacts_updated.zone_my_phone, '') AS zone_my_phone,
         |         NVL(contacts_updated.displayname, '') AS displayname,
         |         NVL(contacts_updated.company, '') AS company,
         |         NVL(contacts_updated.position, '') AS position,
         |         NVL(contacts_updated.phone, '') AS phone,
         |         NVL(contacts_updated.zone_phone, '') AS zone_phone,
         |         NVL(contacts_updated.plat, '') AS plat,
         |         contacts_updated.dt,
         |         contacts_updated.hour
         |  FROM
         |  (
         |    SELECT if(contacts_new.shareid = '',id_mapping_dedup.device,contacts_new.shareid) AS device,
         |           SPLIT(EXTRACT_PHONE_NUM(contacts_new.my_phone), '\\\\|')[0] AS my_phone,
         |           SPLIT(EXTRACT_PHONE_NUM(contacts_new.my_phone), '\\\\|')[1] AS zone_my_phone,
         |           regexp_replace(contacts_new.displayname, '\\\\t|\\\\n|\\\\r', ' ') AS displayname,  --过滤特殊字符
         |           regexp_replace(contacts_new.company, '\\\\t|\\\\n|\\\\r', ' ') AS company,
         |           regexp_replace(contacts_new.position, '\\\\t|\\\\n|\\\\r', ' ') AS position,
         |           SPLIT(EXTRACT_PHONE_NUM(contacts_new.phone), '\\\\|')[0] AS phone,
         |           SPLIT(EXTRACT_PHONE_NUM(contacts_new.phone), '\\\\|')[1] AS zone_phone,
         |           id_mapping_dedup.plat,
         |           contacts_new.dt,
         |           contacts_new.hour,
         |           row_number() over (
         |             PARTITION BY if(contacts_new.shareid = '',id_mapping_dedup.device,contacts_new.shareid), SPLIT(EXTRACT_PHONE_NUM(contacts_new.my_phone), '\\\\|')[0], SPLIT(EXTRACT_PHONE_NUM(contacts_new.phone), '\\\\|')[0]
         |             ORDER BY contacts_new.dt DESC, contacts_new.hour DESC
         |           ) rank
         |    FROM $DM_MOBDI_TMP.phone_contacts contacts_new
         |    LEFT JOIN
         |    id_mapping_dedup ON contacts_new.duid=id_mapping_dedup.duid
         |    WHERE contacts_new.dt BETWEEN '${contacts_start_day}' AND '${contacts_end_day}'
         |    AND LENGTH(TRIM(contacts_new.duid)) > 0 --contacts_new.duid不为空
         |    AND LENGTH(TRIM(id_mapping_dedup.device)) > 0 --id_mapping_dedup.device 不为空
         |    AND LENGTH(TRIM(SPLIT(EXTRACT_PHONE_NUM(contacts_new.phone), '\\\\|')[0] )) > 0  --contacts_new.phone 不为空
         |    AND SPLIT(EXTRACT_PHONE_NUM(contacts_new.my_phone), '\\\\|')[0]
         |    <> SPLIT(EXTRACT_PHONE_NUM(contacts_new.phone), '\\\\|')[0]   --my_phone<>phone
         |  ) contacts_updated
         |  WHERE contacts_updated.rank=1
         |  AND LENGTH(contacts_updated.device) > 0
         |  AND LENGTH(contacts_updated.phone) > 0
         |),
         |
         |contacts_hist_selected AS (  --取出更新用户的历史数据(根据updated_users_dedup的 device，my_phone) (NB!不能使用phone作为SEMI JOIN的条件)
         |  SELECT NVL(contacts_hist.device, '') AS device,
         |         NVL(contacts_hist.my_phone, '') AS my_phone,
         |         NVL(contacts_hist.zone_my_phone, '') AS zone_my_phone,
         |         NVL(contacts_hist.displayname, '') AS displayname,
         |         NVL(contacts_hist.company, '') AS company,
         |         NVL(contacts_hist.position, '') AS position,
         |         NVL(contacts_hist.phone, '') AS phone,
         |         NVL(contacts_hist.zone_phone, '') AS zone_phone,
         |         NVL(contacts_hist.first_add_time, '') AS first_add_time,
         |         NVL(contacts_hist.last_add_time, '') AS last_add_time,
         |         NVL(contacts_hist.last_rm_time, '') AS last_rm_time,
         |         NVL(contacts_hist.exist_flag, '') AS exist_flag,
         |         NVL(contacts_hist.plat, '') AS plat,
         |         NVL(contacts_hist.mac, '') AS mac,
         |         NVL(contacts_hist.imei, '') AS imei,
         |         NVL(contacts_hist.phoneno, '') AS phoneno,
         |         NVL(contacts_hist.phone_view, '') AS phone_view,
         |         NVL(contacts_hist.my_phone_fix, '') AS my_phone_fix,
         |         NVL(contacts_hist.zone_my_phone_fix, '') AS zone_my_phone_fix,
         |         contacts_hist.day,
         |         contacts_hist.hour
         |  FROM $PHONE_CONTACTS_DEDUP_FULL contacts_hist
         |  LEFT SEMI JOIN
         |  (
         |    select device,my_phone
         |    from contacts_updated_dedup
         |    group by device,my_phone
         |  ) as t1 ON (contacts_hist.device=t1.device
         |              AND contacts_hist.my_phone=t1.my_phone
         |              ---- contacts_hist.phone不能和contacts_updated_dedup.phone相等
         |              )
         |  WHERE contacts_hist.day='${last_snapshot_day}'
         |  AND contacts_hist.hour='${last_snapshot_hour}'  --上一个周期
         |),
         |
         |---NB! 当contacts_updated_dedup为空时，无法带入contacts_updated_dedup信息!!
         |contacts_updated_with_hist_selected AS (  --新增数据和历史中包含这些用户的数据进行整合
         |  SELECT NVL(contacts_updated_dedup.device, contacts_hist_selected.device) AS device,
         |         NVL(contacts_updated_dedup.my_phone, contacts_hist_selected.my_phone) AS my_phone,
         |         NVL(contacts_updated_dedup.zone_my_phone, contacts_hist_selected.zone_my_phone) AS zone_my_phone,
         |         NVL(contacts_updated_dedup.displayname, contacts_hist_selected.displayname) AS displayname,
         |         NVL(contacts_updated_dedup.company, contacts_hist_selected.company) AS company,
         |         NVL(contacts_updated_dedup.position, contacts_hist_selected.position) AS position,
         |         NVL(contacts_updated_dedup.phone, contacts_hist_selected.phone) AS phone,
         |         NVL(contacts_updated_dedup.zone_phone, contacts_hist_selected.zone_phone) AS zone_phone,
         |         CASE
         |           WHEN contacts_hist_selected.first_add_time IS NOT NULL THEN contacts_hist_selected.first_add_time
         |           ELSE contacts_updated_dedup.dt
         |         END AS first_add_time, --第一次添加日期，具体到天
         |         CASE
         |           WHEN contacts_updated_dedup.device IS NOT NULL
         |             AND contacts_updated_dedup.my_phone IS NOT NULL
         |             AND contacts_updated_dedup.phone IS NOT NULL
         |             THEN contacts_updated_dedup.dt
         |           ELSE contacts_hist_selected.last_add_time
         |         END AS last_add_time,  --最近一次添加日期，具体到天
         |         CASE
         |           WHEN contacts_updated_dedup.device IS NULL
         |             AND contacts_hist_selected.device IS NOT NULL
         |             AND contacts_updated_dedup.my_phone IS NULL
         |             AND contacts_hist_selected.my_phone IS NOT NULL
         |             AND contacts_updated_dedup.phone IS NULL
         |             AND contacts_hist_selected.phone IS NOT NULL
         |             THEN '${current_snapshot_day}'
         |           ELSE NVL(contacts_hist_selected.last_rm_time, '')
         |         END AS last_rm_time,  --最近一次删除日期，具体到天
         |         CASE
         |           WHEN contacts_updated_dedup.device IS NOT NULL
         |             AND contacts_updated_dedup.my_phone IS NOT NULL
         |             AND contacts_updated_dedup.phone IS NOT NULL
         |             THEN 1
         |           ELSE 0
         |         END AS exist_flag, --是否在当前通讯录内，1 是，0 不是
         |         NVL(contacts_updated_dedup.plat, contacts_hist_selected.plat) AS plat,
         |         NVL(contacts_hist_selected.mac, '') AS mac,
         |         NVL(contacts_hist_selected.imei, '') AS imei,
         |         NVL(contacts_hist_selected.phoneno, '') AS phoneno,
         |         NVL(contacts_hist_selected.phone_view, '') AS phone_view,
         |         NVL(contacts_hist_selected.my_phone_fix, '') AS my_phone_fix,
         |         NVL(contacts_hist_selected.zone_my_phone_fix, '') AS zone_my_phone_fix,
         |         NVL(contacts_updated_dedup.dt, '${current_snapshot_day}') AS day,
         |         NVL(contacts_updated_dedup.hour, '${current_snapshot_hour}') AS hour
         |  FROM contacts_updated_dedup
         |  FULL OUTER JOIN
         |  contacts_hist_selected ON (contacts_updated_dedup.device=contacts_hist_selected.device
         |                            AND contacts_updated_dedup.my_phone=contacts_hist_selected.my_phone
         |                            AND contacts_updated_dedup.phone=contacts_hist_selected.phone)
         |),
         |
         |contacts_updated_and_full_hist_union AS (
         |  SELECT contacts_updated_with_hist_selected.*
         |  FROM contacts_updated_with_hist_selected
         |  UNION ALL
         |  SELECT contacts_hist.*
         |  FROM $PHONE_CONTACTS_DEDUP_FULL contacts_hist
         |  WHERE contacts_hist.day='${last_snapshot_day}'
         |  and contacts_hist.hour='${last_snapshot_hour}'  --上一个周期
         |)
         |
         |SELECT full_union.device,
         |       full_union.my_phone,
         |       full_union.zone_my_phone,
         |       full_union.displayname,
         |       full_union.company,
         |       full_union.position,
         |       full_union.phone,
         |       full_union.zone_phone,
         |       full_union.first_add_time,
         |       full_union.last_add_time,
         |       full_union.last_rm_time,
         |       full_union.exist_flag,
         |       full_union.plat,
         |       full_union.mac,
         |       full_union.imei,
         |       full_union.phoneno,
         |       full_union.phone_view,
         |       full_union.my_phone_fix,
         |       full_union.zone_my_phone_fix
         |FROM
         |(
         |  SELECT contacts_updated_and_full_hist_union.*,
         |         row_number() over (
         |           PARTITION BY contacts_updated_and_full_hist_union.device,
         |           contacts_updated_and_full_hist_union.my_phone,
         |             contacts_updated_and_full_hist_union.phone
         |           ORDER BY contacts_updated_and_full_hist_union.day DESC,
         |           contacts_updated_and_full_hist_union.hour DESC
         |         ) rank
         |  FROM contacts_updated_and_full_hist_union
         |) full_union
         |WHERE full_union.rank=1
         |AND (LENGTH(full_union.my_phone) = 0 OR LENGTH(full_union.my_phone) = 17 OR LENGTH(full_union.my_phone) = 32)
         |AND (LENGTH(full_union.phone) = 17 OR LENGTH(full_union.phone) = 32)
      """.stripMargin


    val conf = new SparkConf().setAppName(
      this.getClass.getSimpleName.stripSuffix("$") + s"_${current_snapshot_day}_${current_snapshot_hour}")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.sql("create temporary function EXTRACT_PHONE_NUM as 'com.youzu.mob.java.udf.PhoneNumExtract'")
    spark.sql("create temporary function explode_tags as 'com.youzu.mob.java.udtf.ExplodeTags'")

    spark.sql(main_sql).createOrReplaceTempView("phone_contacts_dedup_full")

    // dw_mobdi_md.phone_contacts生成的数据会有大量缺失my_phone，
    // 需要从android_id_mapping_view和ios_id_mapping_view中取最新手机号补充进来
    spark.sql(
      s"""
         |INSERT OVERWRITE TABLE $PHONE_CONTACTS_DEDUP_FULL PARTITION
         |(day='${current_snapshot_day}', hour='${current_snapshot_hour}')
         |select device,
         |       my_phone,
         |       collect_list(zone_my_phone)[0] as zone_my_phone,
         |       collect_list(displayname)[0] as displayname,
         |       collect_list(company)[0] as company,
         |       collect_list(position)[0] as position,
         |       phone,
         |       collect_list(zone_phone)[0] as zone_phone,
         |       min(first_add_time) as first_add_time,
         |       max(last_add_time) as last_add_time,
         |       max(last_rm_time) as last_rm_time,
         |       collect_list(exist_flag)[0] as exist_flag,
         |       collect_list(plat)[0] as plat,
         |       collect_list(mac)[0] as mac,
         |       collect_list(imei)[0] as imei,
         |       collect_list(phoneno)[0] as phoneno,
         |       collect_list(phone_view)[0] as phone_view,
         |       collect_list(my_phone_fix)[0] as my_phone_fix,
         |       collect_list(zone_my_phone_fix)[0] as zone_my_phone_fix
         |from
         |(
         |  select device,
         |         my_phone,
         |         zone_my_phone,
         |         displayname,
         |         company,
         |         position,
         |         phone,
         |         zone_phone,
         |         first_add_time,
         |         last_add_time,
         |         last_rm_time,
         |         exist_flag,
         |         plat,
         |         mac,
         |         imei,
         |         phoneno,
         |         phone_view,
         |         my_phone_fix,
         |         zone_my_phone_fix
         |  from phone_contacts_dedup_full
         |  where my_phone != '' or my_phone_fix != ''
         |
         |  union all
         |
         |  select phone_contacts_dedup_full.device,
         |         my_phone,
         |         zone_my_phone,
         |         displayname,
         |         company,
         |         position,
         |         phone,
         |         zone_phone,
         |         first_add_time,
         |         last_add_time,
         |         last_rm_time,
         |         exist_flag,
         |         plat,
         |         coalesce(phone_info_fix.mac,'') as mac,
         |         coalesce(phone_info_fix.imei,'') as imei,
         |         coalesce(phone_info_fix.phoneno,'') as  phoneno,
         |         coalesce(phone_info_fix.phone_view,'') as phone_view,
         |         if(my_phone_id_mapping is null,'',SPLIT(EXTRACT_PHONE_NUM(my_phone_id_mapping), '\\\\|')[0]) as my_phone_fix,
         |         if(my_phone_id_mapping is null,'',SPLIT(EXTRACT_PHONE_NUM(my_phone_id_mapping), '\\\\|')[1]) as zone_my_phone_fix
         |  from phone_contacts_dedup_full
         |  left join
         |  (
         |    select device,my_phone_id_mapping,mac,imei,phoneno,phone_view
         |    from
         |    (
         |      select device,my_phone_id_mapping,phone_tms,mac,imei,phoneno,phone_view,
         |             row_number() over(partition by device order by phone_tms desc) num
         |      from
         |      (
         |        select device,concat(phone,'=',phone_tm) as phone_list,mac,imei,'' as phoneno,phone as phone_view
         |        from $DIM_ID_MAPPING_ANDROID_DF_VIEW
         |        where length(phone)>0
         |        and length(phone)<=10000
         |
         |        union all
         |
         |        select device,concat(phone,'=',phone_tm) as phone_list,mac,idfa as imei,'' as phoneno,phone as phone_view
         |        from $IOS_ID_MAPPING_FULL_VIEW
         |        where length(phone)>0
         |        and length(phone)<=10000
         |      ) phone_info lateral view explode_tags(phone_list) phone_tmp as my_phone_id_mapping,phone_tms
         |    ) un
         |    where num=1
         |  ) phone_info_fix on phone_contacts_dedup_full.device = phone_info_fix.device
         |  where phone_contacts_dedup_full.my_phone = ''
         |  and phone_contacts_dedup_full.my_phone_fix = ''
         |) al
         |group by device,my_phone,phone
       """.stripMargin)

    spark.sql("drop temporary function EXTRACT_PHONE_NUM")

    spark.stop()

  }

}