package com.mob.deviceid.gen

import org.apache.spark.sql.SparkSession

object UniqueIdFull {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder().enableHiveSupport().appName("unique_id_full").getOrCreate()

    // STEP0: mapping表取时间最早的数据
    spark.sql("drop table if exists mobdi_test.device_d2_mapping_min_serdatetime")

    spark.sql(
      s"""
         |create table mobdi_test.device_d2_mapping_min_serdatetime stored as orc as
         |select device,d2
         |from
         |(select device,d2,row_number() over (partition by device order by serdatetime asc) rk
         |from rp_sdk_gapoi_tmp.device_unique_id_mapping
         |where version = '730' and d1 = d2) t
         |where t.rk=1
         |group by t.device,t.d2
         |""".stripMargin)

    // STEP1: 源数据mobdi_test.device_unique_id_info取serdatetime最大值数据
    spark.sql("drop table if exists mobdi_test.device_unique_id_max_serdatetime_v2_all")

    spark.sql(
      s"""
         |create table if not exists mobdi_test.device_unique_id_max_serdatetime_v2_all stored as orc as
         |select device,token,device_new,ieid,mcid,snid,oiid,asid,sysver,factory,max(serdatetime) serdatetime
         |from rp_sdk_gapoi_tmp.device_unique_id_info
         |where day = '20210106'
         |group by device,token,device_new,ieid,mcid,snid,oiid,asid,sysver,factory
         |""".stripMargin)

    // STEP2: group by device_new将其他字段按"|"拼接，最后合并到collect_list中
    spark.sql("drop table if exists mobdi_test.device_unique_id_groupby_other_field_v2_all")

    spark.sql(
      s"""
         |create table if not exists mobdi_test.device_unique_id_groupby_other_field_v2_all stored as orc as
         |select device_new,concat_ws('@#@',collect_list(other_field)) as other_field_list
         |from
         |(select device_new,concat_ws('|',device,token,ieid,mcid,snid,oiid,asid,sysver,factory,serdatetime) as other_field
         |from mobdi_test.device_unique_id_max_serdatetime_v2_all) t
         |group by device_new
         |""".stripMargin)

    // STEP3: 保证源数据device_new唯一后与mapping表关联，此时两表一对一
    spark.sql("drop table if exists mobdi_test.device_unique_id_left_join_d2mapping_v2_all")

    spark.sql(
      s"""
         |create table if not exists mobdi_test.device_unique_id_left_join_d2mapping_v2_all stored as orc as
         |select t.device_new,t.other_field_list,t.combined_device
         |from(
         |select a.device_new,a.other_field_list,COALESCE(b.d2, a.device_new) as combined_device
         |from mobdi_test.device_unique_id_groupby_other_field_v2_all a
         |left join mobdi_test.device_d2_mapping_min_serdatetime b
         |on a.device_new=b.device
         |) t
         |""".stripMargin)

    // STEP4: 将合并后数据展开得到最终结果表
    // spark.sql("drop table if exists mobdi_test.device_unique_id_left_join_result_v2_all")

    spark.sql(
      s"""
         |insert overwrite table mobdi_test.deviceid_new_ids_mapping_full partition(day='20210106')
         |select
         | device_new as device_token,
         | combined_device as muid,
         | fields[0] as device_old,
         | fields[1] as token,
         | if(fields[2] rlike '^ieid', '', fields[2]) as ieid,
         | if(fields[3] rlike '^mcid', '', fields[3]) as mcid,
         | if(fields[4] rlike '^snid', '', fields[4]) as snid,
         | if(fields[5] rlike '^oiid', '', fields[5]) as oiid,
         | if(fields[6] rlike '^asid', '', fields[6]) as asid,
         | fields[7] as sysver,
         | fields[8] as factory,
         | fields[9] as serdatetime
         |from
         |(select device_new,combined_device,split(other_field,'\\\\|') as fields
         |from
         |(select device_new,other_field_list,combined_device from mobdi_test.device_unique_id_left_join_d2mapping_v2_all) t
         |lateral view explode(split(other_field_list,'@#@')) f as other_field
         |) tt
         |""".stripMargin)

}

}
