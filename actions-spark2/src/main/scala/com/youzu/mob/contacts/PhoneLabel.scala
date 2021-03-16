package com.youzu.mob.contacts

import com.youzu.mob.utils.Constants.{DM_MOBDI_TMP, PHONE_CONTACTS_DEDUP_FULL, PHONE_LABEL}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object PhoneLabel {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    if (args.length != 1) {
      println("parameter error")
      sys.exit(-1)
    }

    val insert_day = args(0)

    val main_sql =
      s"""
         |     WITH phone_device_mapping AS (
         |     SELECT  case when length(regexp_extract(t1.my_phone, '0008601(3|4|5|6|7|8|9)[0-9]{9}', 0))>0
         |                then substr(regexp_extract(t1.my_phone, '0008601(3|4|5|6|7|8|9)[0-9]{9}', 0), 7, 17)
         |                when length(regexp_extract(t1.my_phone, '000852000([6|9])[0-9]{7}', 0))>0
         |                then regexp_extract(t1.my_phone, '000852000([6|9])[0-9]{7}', 0)
         |                 when length(regexp_extract(t1.my_phone, '000886009[0-9]{8}', 0))>0
         |                  then regexp_extract(t1.my_phone, '000886009[0-9]{8}', 0)
         |                when length(regexp_extract(t1.my_phone, '000853[0-9]{11}', 0))>0
         |                  then regexp_extract(t1.my_phone, '000853[0-9]{11}', 0)
         |                 when length(regexp_extract(t1.my_phone, '0000010[0-9]{10}', 0))>0
         |                then regexp_extract(t1.my_phone, '0000010[0-9]{10}', 0)
         |                else '' end as my_phone,
         |            t1.device as device,
         |            t1.plat as plat
         |     FROM
         |     (
         |       SELECT my_phone,device,plat,
         |              row_number() over (PARTITION BY my_phone ORDER BY day DESC, hour DESC) rank
         |       FROM
         |       (
         |         select if(my_phone='',my_phone_fix,my_phone) as my_phone,
         |                device,plat,day,hour
         |         from $DM_MOBDI_TMP.phone_contacts
         |               WHERE day='$insert_day'
         |         AND (LENGTH(my_phone) > 0 or LENGTH(my_phone_fix) > 0)
         |       ) info
         |     ) t1
         |     WHERE t1.rank = 1
         |   ),
         |   full_info AS (
         |           SELECT  case  when length(regexp_extract(phone, '0008601(3|4|5|6|7|8|9)[0-9]{9}', 0))>0
         |                then substr(regexp_extract(phone, '0008601(3|4|5|6|7|8|9)[0-9]{9}', 0), 7, 17)
         |                 when length(regexp_extract(phone, '000852000([6|9])[0-9]{7}', 0))>0
         |                 then regexp_extract(phone, '000852000([6|9])[0-9]{7}', 0)
         |                 when length(regexp_extract(phone, '000886009[0-9]{8}', 0))>0
         |                 then regexp_extract(phone, '000886009[0-9]{8}', 0)
         |                 when length(regexp_extract(phone, '000853[0-9]{11}', 0))>0
         |                 then regexp_extract(phone, '000853[0-9]{11}', 0)
         |                when length(regexp_extract(phone,'0000010[0-9]{10}', 0))>0
         |                then regexp_extract(phone,'0000010[0-9]{10}', 0)
         |                            else '' end as phone,
         |                  trim(regexp_replace(
         |                  regexp_replace(
         |                 CASE WHEN length(displayname) > 0 THEN displayname
         |                     WHEN length(company) > 0 THEN company
         |                     ELSE position
         |                END
         |              , ',', ' ')
         |                  , '\\\\s+', ' ')) AS label,
         |            CASE
         |                      WHEN LENGTH(substr(regexp_extract(coalesce(my_phone_fix,my_phone,''), '0008601(3|4|5|6|7|8|9)[0-9]{9}', 0), 7, 17)) = 11
         |            THEN substr(regexp_extract(coalesce(my_phone_fix,my_phone,''), '0008601(3|4|5|6|7|8|9)[0-9]{9}', 0), 7, 17)
         |                      WHEN LENGTH(regexp_extract(coalesce(my_phone_fix,my_phone,''), '000852000([6|9])[0-9]{7}', 0)) = 17
         |            THEN regexp_extract(coalesce(my_phone_fix,my_phone,''), '000852000([6|9])[0-9]{7}', 0)
         |                      WHEN LENGTH(regexp_extract(coalesce(my_phone_fix,my_phone,''), '000886009[0-9]{8}', 0)) = 17
         |            THEN regexp_extract(coalesce(my_phone_fix,my_phone,''), '000886009[0-9]{8}', 0)
         |                      WHEN LENGTH(regexp_extract(coalesce(my_phone_fix,my_phone,''), '000853[0-9]{11}', 0)) = 17
         |            THEN regexp_extract(coalesce(my_phone_fix,my_phone,''), '000853[0-9]{11}', 0)
         |           WHEN LENGTH(regexp_extract(coalesce(my_phone_fix,my_phone,''), '0000010[0-9]{10}', 0)) = 17
         |             THEN regexp_extract(coalesce(my_phone_fix,my_phone,''), '0000010[0-9]{10}', 0)
         |            ELSE 'null' END AS labeled_phone
         |     FROM $DM_MOBDI_TMP.phone_contacts
         |           WHERE day='$insert_day'
         |     AND LENGTH(phone)=17
         |     AND (LENGTH(regexp_extract(phone, '0008601(3|4|5|6|7|8|9)[0-9]{9}', 0)) = 17
         |                    or LENGTH(regexp_extract(phone, '000852000([6|9])[0-9]{7}', 0)) = 17
         |                                or LENGTH(regexp_extract(phone, '000886009[0-9]{8}', 0)) = 17
         |                                or LENGTH(regexp_extract(phone, '000853[0-9]{11}', 0)) = 17
         |         or LENGTH(regexp_extract(phone, '0000010[0-9]{10}', 0)) = 17
         |             ))
         |		 insert overwrite table $PHONE_LABEL partition (day='$insert_day')
         |   SELECT NVL(phone_device_mapping.device, '') AS device,
         |          t1.phone,
         |          t1.label,
         |          NVL(phone_device_mapping.plat, '') AS plat,
         |          t1.label_rate,
         |          t1.labeled_phone
         |   FROM
         |   (
         |     SELECT phone,
         |            regexp_replace(concat_ws(',', collect_list(label)), ',+', ',') as label,
         |            concat_ws(',', collect_list(cast(label_rate as string))) as label_rate,
         |            concat_ws(',',collect_list(labeled_phone)) as labeled_phone
         |     FROM
         |     (
         |       select phone,label,labeled_phone,
         |              cast(round(cast(cnt as double)/sum(cnt) over(partition by phone),4) as decimal(18,4)) as label_rate
         |       from
         |       (
         |         select phone,label,concat_ws('_',sort_array(collect_set(labeled_phone))) as labeled_phone,count(1) as cnt
         |         from full_info
         |         where label != ''
         |         group by phone,label
         |       ) full_cnt_info
         |     ) full_rate_info
         |     GROUP BY phone
         |   ) t1
         |   LEFT JOIN
         |   phone_device_mapping ON (t1.phone=phone_device_mapping.my_phone)
         |   union all
         |   select NVL(phone_device_mapping.device,'') AS device,
         |          t1.phone,
         |          t1.label,
         |          NVL(phone_device_mapping.plat, '') AS plat,
         |          '' as label_rate,
         |          '' as labeled_phone
         |   from
         |   (
         |     select phone,
         |            regexp_replace(concat_ws(',', collect_set(label)), ',+', ',') as label
         |     from full_info
         |     group by phone
         |   ) t1
         |   LEFT JOIN
         |   phone_device_mapping ON (t1.phone=phone_device_mapping.my_phone)
         |   where t1.label = ''
       """.stripMargin

    val conf = new SparkConf().setAppName(this.getClass.getSimpleName.stripSuffix("$") + s"_$insert_day")

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    spark.sql(main_sql)

    spark.stop()
  }
}
