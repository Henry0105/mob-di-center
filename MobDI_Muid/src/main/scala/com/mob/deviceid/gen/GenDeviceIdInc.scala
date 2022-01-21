package com.mob.deviceid.gen

import com.mob.deviceid.config.HiveProps
import com.mob.deviceid.udf._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.spark.storage.StorageLevel

object GenDeviceIdInc {
  private val LOGGER: Logger = LoggerFactory.getLogger("GenDeviceIdIncOld")



  def sql(spark: SparkSession, s: String): DataFrame = {
    LOGGER.info(
      s"""
         |------------execute sql------------
         |$s
         |-----------------------------------
         |""".stripMargin)
    spark.sql(s)
  }

  def newTable(spark: SparkSession, t: String, s: String): Unit = {
    LOGGER.info(
      s"""
         |------------execute sql as $t------------
         |$s
         |-----------------------------------
         |""".stripMargin)
    spark.sql(s).createOrReplaceTempView(t)
  }

  def main(args: Array[String]): Unit = {
    import HiveProps._
    val spark: SparkSession = SparkSession.builder().enableHiveSupport().appName("unique_id").getOrCreate()

    val day = args(0)
    spark.udf.register("factory_classify", (factory: String) => new FactoryUDF().factory_classify(factory))
    spark.udf.register("is_not_blank", (str: String) => new StringUDF().isNotBlank(str))
    spark.udf.register("is_blank", (str: String) => new StringUDF().isBlank(str))

    spark.udf.register("luhn_checker", (str: String) => new LuhnCheckerUDF().luhn_checker(str))
    spark.udf.register("ieid_verify", (str: String) => new ImeiVerifyUDF().ieid_verify(str))
    spark.udf.register("sha", (str: String) => new SHA1HashingUDF().sha(str))

    // TODO 换线上full mapping表时取最大分区where day='$maxPartitionDay'
    // val partition_df = spark.sql( s"show partitions $DEVICEID_NEW_IDS_MAPPING_FULL" )
    // val maxPartition = partition_df.selectExpr( "max(partition) max_partition" )
    // val maxPartitionDay = maxPartition.first().getString(0).split("=")(1)

    // 进行id生成的表数据
    // jh表需要用到的字段:serdatetime,device,adsid,imei,serialno,factory,sysver,oaid,token
    // 数据清洗，目前线上的规则
    // serdatetime,device_old,oaid,adsid,ieid,snid,mcid,sysver,token,factory
    spark.sql(
      s"""
         |select device,token,
         |       if(b1.value is not null,'',imei) as imei,
         |       if(b2.value is not null,'',mac) as mac,
         |       if(b3.value is not null,'',serialno) as serialno,
         |       case when lower(trim(oaid)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or oaid is null then ''
         |            when trim(oaid) rlike '^([A-Za-z0-9]|-)+$$' then trim(oaid)
         |            else '' end as oaid,
         |       case when lower(trim(adsid)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or adsid is null then ''
         |            when regexp_replace(lower(trim(adsid)), ' |-|\\\\.|:|\073','') rlike '0{32}' then ''
         |            when regexp_replace(lower(trim(adsid)), ' |-|\\\\.|:|\073','') rlike '^[0-9a-f]{32}$$'
         |             then concat(substring(regexp_replace(trim(upper(adsid)), ' |-|\\\\.|:|\073', '') , 1, 8), '-',substring(regexp_replace(trim(upper(adsid)), ' |-|\\\\.|:|\073', '') , 9, 4), '-',substring(regexp_replace(trim(upper(adsid)), ' |-|\\\\.|:|\073', '') , 13, 4), '-',substring(regexp_replace(trim(upper(adsid)), ' |-|\\\\.|:|\073', '') , 17, 4), '-',substring(regexp_replace(trim(upper(adsid)), ' |-|\\\\.|:|\073', '') , 21, 12))
         |            else '' end as adsid,
         |       sysver,
         |       case when trim(upper(split(factory, ' ')[0])) in ('HUAWEI','HONOR','OPPO','VIVO','XIAOMI','REDMI','SAMSUNG','INFINIX','MEIZU','MEILAN','ONEPLUS','GIONEE','MOTOROLA','ZTE','NUBIA','LENOVO','LEMOBILE') then trim(upper(split(factory, ' ')[0]))
         |            else 'other' end as factory,
         |       serdatetime
         |from
         |(select device,token,
         |if(luhn_checker(case when trim(imei) rlike '0{14,17}' then ''
         |      when length(trim(lower(imei))) = 16 and trim(imei) not rlike '^[0-9]+$$' then ''
         |	    when length(trim(lower(imei))) = 16 and trim(imei) rlike '^[0-9]+$$'
         |           then if(ieid_verify(substring(regexp_replace(trim(lower(imei)), ' |/|-|imei:', ''),1,14)),regexp_replace(trim(lower(imei)), ' |/|-|imei:', ''),'')
         |	    when ieid_verify(regexp_replace(trim(lower(imei)), ' |/|-|imei:', '')) then regexp_replace(trim(lower(imei)), ' |/|-|imei:', '')
         |	         else '' end), regexp_replace(trim(lower(imei)), ' |/|-|imei:', ''),'') as imei,
         |case when trim(lower(mac)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or mac is null then ''
         |     when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') in ('000000000000', '020000000000') then ''
         |     when regexp_replace(trim(lower(mac)), ' |-|\\\\.|:|\073', '') rlike '^[0-9a-f]{12}$$'
         |      then substring(regexp_replace(regexp_replace(trim(lower(mac)),
         |       ' |-|\\\\.|:|\073', ''), '(.{2})', '\\$$1:'), 1, 17)
         |     else '' end as mac,
         |case when lower(trim(serialno)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other') or serialno is null then ''
         |     when lower(trim(serialno)) rlike '^[0-9a-z]{6,32}$$' then lower(trim(serialno))
         |     else '' end as serialno,
         |oaid,adsid,sysver,factory,serdatetime
         |from
         |  (
         |   select device,token,imei,mac,serialno,oaid,adsid,sysver,factory,serdatetime
         |   from
         |   (
         |     select device,token,imei,mac,serialno,oaid,adsid,sysver,factory,serdatetime,
         |     row_number() over(partition by device,token,imei,mac,serialno,oaid,adsid,sysver,factory order by serdatetime desc) as rank
         |     from dw_sdk_log.log_device_info_jh
         |     where dt='${day}' and plat=1
         |           and device is not null
         |           and device rlike '[a-f0-9]{40}'
         |           and device!='0000000000000000000000000000000000000000'
         |           and (serialno is not null or imei is not null or mac is not null or oaid is not null or adsid is not null)
         |   ) s
         |   where s.rank=1
         |  ) t
         |) a
         |left join
         |(select value from dm_sdk_mapping.blacklist_view where type='imei' and value is not null and value!='') b1
         |on a.imei=b1.value
         |left join
         |(select value from dm_sdk_mapping.blacklist_view where type='mac' and value is not null and value!='') b2
         |on a.mac=b2.value
         |left join
         |(select value from dm_sdk_mapping.blacklist_view where type='serialno' and value is not null and value!='') b3
         |on a.serialno=b3.value
         |""".stripMargin).createOrReplaceTempView("muid_incr_source_table")

    // jh表数据清洗,对imei, mac, serialno, oaid, adsid异常值过滤(基于分析师给的逻辑，如遇到异常值则置为空)
    val genIdRawSql =
      s"""
         |select serdatetime,device_old,oaid,adsid,ieid,snid,mcid,sysver,token,factory
         |from
         |(
         |select
         |  serdatetime,
         |  device as device_old,
         |  case when oaid in ('00000000-0000-0000-0000-000000000000', '00000000000000000000000000000000')
         |       then ''
         |       else oaid
         |       end as oaid,
         |  case when adsid in ('00000000-0000-0000-0000-000000000000', '4c5f81a0-4728-476f-a57f-b46fa44f07d3', 'b49d70ac-8f33-4e4a-ae14-141323163815',
         |                      '814db7a6-9354-458c-823a-d16736ace54b','c0e47fec-2846-40be-88e4-6c14ef761f2f', 'c610a69c-2c40-447c-877f-950c9932ba54',
         |                      'f8a5056c-d57c-4f1a-ae1b-013ae7dd7c62')
         |       then ''
         |       else adsid
         |       end as adsid,
         |  case when imei in ('00000000000000000', '0000000000000000', '000000000000000', '00000000000000', '861622010000056')
         |       then ''
         |       else imei
         |       end as ieid,
         |  case when serialno in ('unknown', '0123456789abcdef', '27876933', 'android', 'zx1g42cpjd', 'mt3276829', '93975465', '8901260496494269244',
         |                         '37794573', '01234567890123456789', 'deface', '19022064', 'zteba510', '64220073', 'ztebv0730', 'zteba520', '33373971',
         |                         'zteba601', '33532045', 'ztebv0720', '12345678900', '0123456789', '123456789', 'zteb880', 'zteba610t', 'zteba611t',
         |                         'zteba910', 'ztebv0710', 'ztebv0710t', 'ztec880a', 'ztec880s', 'zteq529c', 'zteq529t')
         |       then ''
         |       else serialno
         |       end as snid,
         |  case when mac in ('02:00:00:00:00:00', '00:00:00:00:00:00', 'ff:ff:ff:ff:ff:ff', '58:02:03:04:05:06', '58:02:03:04:05:09', '58:02:03:04:05:07',
         |                    '00:02:00:00:00:00', 'a6:c0:80:e4:1a:50', '08:00:27:a9:d5:97', '08:00:27:b2:8b:50', '00:81:df:d5:a6:a5', '90:67:1c:e6:4d:55',
         |                    '00:81:3c:75:32:e1', '08:00:27:0e:38:b3', '00:90:4c:11:22:33', '29:a1:9b:61:cc:d8', 'dc:35:67:ca:55:00', '08:00:27:c4:46:c0',
         |                    '00:81:f6:67:bf:f3', '14:63:18:3d:ab:18', '11:22:33:44:55:66', '00:81:4d:13:47:09', '00:81:b4:a5:b8:20', 'd4:97:0b:64:eb:8e',
         |                    '2c:28:2d:43:ac:27', '8c:be:be:ba:57:89', '08:7a:4c:76:fa:54', '08:00:27:f1:b1:95', 'cf:9b:b5:6a:60:de', '00:81:a2:68:a4:c4',
         |                    '28:58:4e:8f:22:a1', '14:f6:5a:8e:8e:67')
         |         then ''
         |         else mac
         |         end as mcid,
         |  sysver,
         |  token,
         |  factory_classify(factory) factory
         |from muid_incr_source_table
         |) t
         |group by serdatetime,device_old,oaid,adsid,ieid,snid,mcid,sysver,token,factory
         |""".stripMargin

    val genIdRawDF = spark.sql(genIdRawSql).persist(StorageLevel.MEMORY_AND_DISK)

    val gen_id_raw = "jh_table_remove_exception_value"
    genIdRawDF.createOrReplaceTempView(gen_id_raw)
    // newTable(spark, gen_id_raw, genIdRawSql)
    // 增量黑名单逻辑
    val muid_ieid_mcid_mapping = "dm_mobdi_mapping.muid_ieid_mcid_mapping_incr"
    val muid_ieid_snid_mapping = "dm_mobdi_mapping.muid_ieid_snid_mapping_incr"
    val muid_mcid_snid_mapping = "dm_mobdi_mapping.muid_mcid_snid_mapping_incr"

    //这个分区是全量分区
    val mappingPar = "20210329"
    // 更新黑名单映射mapping表

    spark.sql(
      s"""
         |insert overwrite table $muid_ieid_mcid_mapping partition(day='$day')
         |select ieid, mcid from $gen_id_raw
         |where ieid != '' and mcid != ''
         |group by ieid, mcid
         |""".stripMargin
    )
    spark.sql(
      s"""
         |insert overwrite table $muid_ieid_snid_mapping partition(day='$day')
         |select ieid, snid from $gen_id_raw
         |where ieid != '' and snid != ''
         |group by ieid, snid
         |""".stripMargin
    )
    spark.sql(
      s"""
         |insert overwrite table $muid_mcid_snid_mapping partition(day='$day')
         |select mcid, snid from $gen_id_raw
         |where  mcid != '' and snid != ''
         |group by mcid, snid
         |""".stripMargin
    )

    // 黑名单匹配
    val ieid_blacklist =
      s"""
         |select ieid
         |from (
         |    select ieid
         |    from (
         |        select ieid, count(1)
         |        from (
         |            select ieid, mcid
         |            from $muid_ieid_mcid_mapping
         |            where day>= '$mappingPar'
         |            group by ieid, mcid
         |        )t
         |        group by ieid
         |        having count(1) > 2
         |    )t1
         |
         |    union all
         |
         |    select ieid
         |    from (
         |        select ieid, count(1)
         |        from (
         |            select ieid, snid
         |            from $muid_ieid_snid_mapping
         |            where day>= '$mappingPar'
         |            group by ieid, snid
         |        )t
         |        group by ieid
         |        having count(1) > 2
         |    )t2
         |)t3
         |group by ieid
         |""".stripMargin
    newTable(spark, "ieid_blacklist", ieid_blacklist)
    val mcid_blacklist =
      s"""
         |select mcid
         |from (
         |    select mcid
         |    from (
         |        select mcid, count(1)
         |        from (
         |            select ieid, mcid
         |            from $muid_ieid_mcid_mapping
         |            where day>= '$mappingPar'
         |            group by ieid, mcid
         |        )t
         |        group by mcid
         |        having count(1) > 3
         |    )t1
         |
         |    union all
         |
         |    select mcid
         |    from (
         |        select mcid, count(1)
         |        from (
         |            select mcid, snid
         |            from $muid_mcid_snid_mapping
         |            where day>= '$mappingPar'
         |            group by mcid, snid
         |        )t
         |        group by mcid
         |        having count(1) > 2
         |    )t2
         |)t3
         |group by mcid
         |""".stripMargin
    newTable(spark, "mcid_blacklist", mcid_blacklist)
    val snid_blacklist =
      s"""
         |select snid
         |from (
         |    select snid
         |    from (
         |        select snid, count(1)
         |        from (
         |            select ieid, snid
         |            from $muid_ieid_snid_mapping
         |            where day>= '$mappingPar'
         |            group by ieid, snid
         |        )t
         |        group by snid
         |        having count(1) > 3
         |    )t1
         |
         |    union all
         |
         |    select snid
         |    from (
         |        select snid, count(1)
         |        from (
         |            select mcid, snid
         |            from $muid_mcid_snid_mapping
         |            where day>= '$mappingPar'
         |            group by mcid, snid
         |        )t
         |        group by snid
         |        having count(1) > 2
         |    )t2
         |)t3
         |group by snid
         |""".stripMargin
    newTable(spark, "snid_blacklist", snid_blacklist)


    newTable(spark, "snid_bl",
      s"select snid from (select snid from snid_blacklist union all select snid from $SNID_BLACKLIST) t group by snid")
    newTable(spark, "imei_bl",
      s"select ieid from (select ieid from ieid_blacklist union all select ieid from $IEID_BLACKLIST) t group by ieid")
    newTable(spark, "mcid_bl",
      s"select mcid from (select mcid from mcid_blacklist union all select mcid from $MCID_BLACKLIST) t group by mcid")


    // STEP1: 生成黑名单数据
    // 集群对于数据量有些大且长链路的Stage必失败，只能采取这样落地到临时表
    val blacklistStep1Table = "dw_mobdi_md.source_left_join_snid_blacklist_p1"
    spark.sql(s"drop table if exists $blacklistStep1Table")
    spark.sql(
      s"""
         |create table if not exists $blacklistStep1Table stored as orc as
         |select serdatetime,device_old,a.oaid,a.adsid,a.ieid,a.mcid,a.snid,sysver,token,factory,
         |if(b.snid is not null,1,0) as snid_flag
         |from
         |(select serdatetime,device_old,oaid,adsid,ieid,snid,mcid,sysver,token,factory
         |from $gen_id_raw
         |where snid is not null and snid!='unknown') a
         |left join snid_bl b on a.snid = b.snid
         |union all
         |select serdatetime,device_old,oaid,adsid,ieid,snid,mcid,sysver,token,factory,0 as snid_flag
         |from $gen_id_raw
         |where snid is null or snid='unknown'
         |""".stripMargin
    )
    // blacklistStep1DF.createOrReplaceTempView(blacklistStep1Table)


    val blacklistStep2Table = "dw_mobdi_md.source_left_join_snid_blacklist_p2"
    spark.sql(s"drop table if exists $blacklistStep2Table")
    spark.sql(
      s"""
         |create table if not exists $blacklistStep2Table stored as orc as
         |select serdatetime,device_old,a.oaid,a.adsid,a.ieid,a.mcid,a.snid,sysver,token,factory,snid_flag,
         |if(b.ieid is not null,1,0) as ieid_flag
         |from
         |(select serdatetime,device_old,oaid,adsid,ieid,snid,mcid,sysver,token,factory,snid_flag
         |from $blacklistStep1Table
         |where ieid is not null and ieid!='unknown') a
         |left join imei_bl b on a.ieid = b.ieid
         |union all
         |select serdatetime,device_old,oaid,adsid,ieid,snid,mcid,sysver,token,factory,snid_flag,0 as ieid_flag
         |from $blacklistStep1Table
         |where ieid is null or ieid='unknown'
         |""".stripMargin
    )
    // blacklistStep2DF.createOrReplaceTempView(blacklistStep2Table)



    val blacklistStep3Table = "dw_mobdi_md.source_left_join_blacklist_day"
    spark.sql(s"drop table if exists $blacklistStep3Table")
    spark.sql(
      s"""
         |create table if not exists $blacklistStep3Table stored as orc as
         |select serdatetime,device_old,a.oaid,a.adsid,a.ieid,a.mcid,a.snid,sysver,token,factory,snid_flag,ieid_flag,
         |if(b.mcid is not null,1,0) as mcid_flag
         |from
         |(select serdatetime,device_old,oaid,adsid,ieid,snid,mcid,sysver,token,factory,snid_flag,ieid_flag
         |from $blacklistStep2Table
         |where mcid is not null and mcid!='unknown') a
         |left join mcid_bl b on a.mcid = b.mcid
         |union all
         |select serdatetime,device_old,oaid,adsid,ieid,snid,mcid,sysver,token,factory,snid_flag,ieid_flag,0 as mcid_flag
         |from $blacklistStep2Table
         |where mcid is null or mcid='unknown'
         |""".stripMargin
    )
    //blacklistStep3DF.createOrReplaceTempView(blacklistStep3Table)

    // 原始数据跟各个黑名单表进行join,新增一个flag,join上黑名单里的任意一个,就把flag置为1,否则为0
    val joinWithBl = spark.sql(
      s"""
         |select serdatetime,device_old,oaid,adsid,ieid,mcid,snid,sysver,token,factory,cast(null as string) as muid,
         | if(snid_flag=1 or ieid_flag=1 or mcid_flag=1,1,0) as flag
         |from $blacklistStep3Table
         |""".stripMargin)
    genIdRawDF.unpersist()

    // 非黑名单数据
    val normalRaw = joinWithBl.where("flag=0").drop("flag")
    normalRaw.persist(StorageLevel.MEMORY_AND_DISK)
    // oaid不为空的数据
    val oaid_not_null = "oaid_not_null"
    normalRaw.where("is_not_blank(oaid)").createOrReplaceTempView(oaid_not_null)

    // oaid为空的数据
    val oaidNull = normalRaw.where("is_blank(oaid)")

    val muidFullMapping = "muid_full_mapping"
    val mappingFullPartition = "20210323"

    val muidFullMappingDF = spark.sql(
      s"""
         |select muid,ieid,oiid,asid,factory,serdatetime
         |from $DEVICEID_NEW_IDS_MAPPING_FULL where day='$mappingFullPartition'
         |union all
         |select muid,ieid,oiid,asid,factory,serdatetime
         |from $DEVICEID_NEW_IDS_MAPPING_INCR where day>'20210323'
         |""".stripMargin)
    //muidFullMappingDF.persist(StorageLevel.MEMORY_AND_DISK)
    muidFullMappingDF.createOrReplaceTempView(muidFullMapping)


    // 取oaid不为空的数据,根据oaid,factory取muid
    val oaidFactoryJoinTable = "oaid_factory_left_join_p3"
    val oaidFactoryJoinDF = spark.sql(
      s"""
         |select a.oaid,a.factory,b.muid
         |from
         |(select oaid,factory from $oaid_not_null group by oaid,factory) a
         |left join
         |(select muid,oiid,factory
         |from
         |(select muid,oiid,factory,
         |  row_number() over (partition by oiid,factory order by serdatetime desc) rk
         |from $muidFullMapping
         |) t
         |where t.rk=1
         |) b
         |on a.factory=b.factory and a.oaid=b.oiid
         |""".stripMargin
    )
    oaidFactoryJoinDF.createOrReplaceTempView(oaidFactoryJoinTable)

    val rawWithDeviceIdNewByOaidSql =
      s"""
         |select a.serdatetime,a.device_old,a.oaid,adsid,ieid,mcid,snid,sysver,token,a.factory,b.muid
         |from $oaid_not_null a
         |left join $oaidFactoryJoinTable b on a.oaid=b.oaid and a.factory=b.factory
         |""".stripMargin
    val rawWithDeviceIdNewByOaid = sql(spark, rawWithDeviceIdNewByOaidSql).persist(StorageLevel.MEMORY_AND_DISK)

    // 使用oaid匹到的记录
    val rawWithOaidDeviceidFinal = rawWithDeviceIdNewByOaid.where("muid is not null")

    // 使用oaid没匹到的记录
    val rawNoDeviceIdNewByOaid = rawWithDeviceIdNewByOaid.where("muid is null")

    /* ------------------adsid--------------------- */

    val rawWithAdsid = rawNoDeviceIdNewByOaid.unionByName(oaidNull)
    rawWithAdsid.persist(StorageLevel.MEMORY_AND_DISK)
    val adsid_not_null = "adsid_not_null"
    rawWithAdsid.where("is_not_blank(adsid)").createOrReplaceTempView(adsid_not_null)

    // 没adsid的
    val adsidNull = rawWithAdsid.where("is_blank(adsid)")

    // 取adsid不为空的数据,根据adsid,factory取deviceid
    // spark.sql("drop table if exists mobdi_test.hugl_adsid_factory_left_join_p3")
    val adsidFactoryJoinTable = "adsid_factory_left_join_p3"
    val adsidFactoryJoinDF = spark.sql(
      s"""
         |select a.adsid,a.factory,b.muid
         |from
         |(select adsid,factory from $adsid_not_null group by adsid,factory) a
         |left join
         |(select muid,asid,factory
         |from
         |(select muid,asid,factory,
         | row_number() over (partition by asid,factory order by serdatetime desc) rk
         |from $muidFullMapping
         |where asid is not null) t
         |where t.rk=1
         |) b
         |on a.factory=b.factory and a.adsid=b.asid
         |""".stripMargin
    )
    adsidFactoryJoinDF.createOrReplaceTempView(adsidFactoryJoinTable)

    val rawWithDeviceIdNewByAdsidSql =
      s"""
         |select a.serdatetime,a.device_old,a.oaid,a.adsid,ieid,a.mcid,a.snid,a.sysver,a.token,a.factory,b.muid
         |from $adsid_not_null a
         |left join $adsidFactoryJoinTable b
         |on a.adsid=b.adsid and a.factory=b.factory
         |""".stripMargin

    val rawWithDeviceIdNewByAdsid = sql(spark, rawWithDeviceIdNewByAdsidSql)
    rawWithDeviceIdNewByAdsid.persist(StorageLevel.MEMORY_AND_DISK)
    // 使用adsid匹到的记录
    val rawWithAdsidDeviceidFinal = rawWithDeviceIdNewByAdsid.where("muid is not null")

    val rawNoDeviceIdNewByAdsid = rawWithDeviceIdNewByAdsid.where("muid is null")


    /* ------------------imei--------------------- */

    val rawWithImei = rawNoDeviceIdNewByAdsid.unionByName(adsidNull)
    rawWithImei.persist(StorageLevel.MEMORY_AND_DISK)
    val imei_not_null = "imei_not_null"
    rawWithImei.where("is_not_blank(ieid)").createOrReplaceTempView(imei_not_null)

    // 没imei的
    val imeiNull = rawWithImei.where("is_blank(ieid)")

    // 取imei不为空的数据,根据imei,factory取deviceid
    val ieidFactoryJoinTable = "ieid_factory_left_join_p3"
    val ieidFactoryJoinDF = spark.sql(
      s"""
         |select a.ieid,a.factory,b.muid
         |from
         |(select ieid,factory from $imei_not_null group by ieid,factory) a
         |left join
         |(select muid,ieid,factory
         |from
         |(select muid,ieid,factory,
         | row_number() over (partition by ieid,factory order by serdatetime desc) rk
         |from $muidFullMapping
         |where ieid is not null) t
         |where t.rk=1
         |) b
         |on a.factory=b.factory and a.ieid=b.ieid
         |""".stripMargin
    )
    ieidFactoryJoinDF.createOrReplaceTempView(ieidFactoryJoinTable)

    val rawWithDeviceIdNewByImeiSql =
      s"""
         |select a.serdatetime,a.device_old,a.oaid,a.adsid,a.ieid,a.snid,a.mcid,a.sysver,a.token,a.factory,b.muid
         |from $imei_not_null a
         |left join $ieidFactoryJoinTable b on a.ieid=b.ieid and a.factory=b.factory
         |""".stripMargin

    val rawWithDeviceIdNewByImei = sql(spark, rawWithDeviceIdNewByImeiSql)
    rawWithDeviceIdNewByImei.persist(StorageLevel.MEMORY_AND_DISK)
    // 使用imei匹到的记录
    val rawWithImeiDeviceidFinal = rawWithDeviceIdNewByImei.where("muid is not null")
    val rawNoDeviceIdNewByImei = rawWithDeviceIdNewByImei.where("muid is null")

    /* ------------------匹配完成--------------------- */
    // 匹配到的记录
    val rawWithExistDeviceid = rawWithImeiDeviceidFinal
      .unionByName(rawWithAdsidDeviceidFinal)
      .unionByName(rawWithOaidDeviceidFinal)
    rawWithExistDeviceid.createOrReplaceTempView("jh_matched_records")

    spark.sql(
      s"""
         |select
         |  device_old,
         |  case when (sysver_clean like '1%' and sysver_clean>='10') and token is not null then sha(token) else device_old end as device_token,
         |  muid,
         |  token,
         |  ieid,
         |  mcid,
         |  snid,
         |  oiid,
         |  asid,
         |  sysver_clean as sysver,
         |  factory,
         |  serdatetime
         |from
         |(select device_old,muid,token,ieid,mcid,snid,oaid as oiid,adsid as asid,a.sysver,factory,serdatetime,
         |case when lower(trim(a.sysver)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other', '未知') or a.sysver is null then 'unknown'
         |  when lower(a.sysver) not rlike '^([0-9a-z]|\\\\.|-| )+$$' then 'unknown'
         |  when a.sysver = b.vernum then b.sysver
         |  when concat(split(regexp_replace(lower(a.sysver), 'android| ', ''), '\\\\.')[0], '.', split(regexp_replace(lower(a.sysver), 'android| ', ''), '\\\\.')[1]) in ('1.0', '1.1', '1.5', '1.6', '2.0', '2.1', '2.2', '2.3', '3.0', '3.1', '3.2', '4.0', '4.1', '4.2', '4.3', '4.4', '4.4w', '5.0', '5.1', '6.0', '7.0', '7.1', '8.0', '8.1', '9.0', '10.0', '11.0') then concat(split(regexp_replace(lower(a.sysver), 'android| ', ''), '\\\\.')[0], '.', split(regexp_replace(lower(a.sysver), 'android| |w', ''), '\\\\.')[1])
         |else 'unknown' end as sysver_clean
         |from jh_matched_records a
         |left join
         |(select sysver,vernum from dm_sdk_mapping.sysver_mapping_par where version='1004') b
         |on a.sysver=b.vernum
         |) t
         |""".stripMargin).createOrReplaceTempView("jh_matched_records_sysver_etl")

    // 这部分为新增的deviceid
    val rawWithNewDeviceId = rawNoDeviceIdNewByImei.unionByName(imeiNull)
    rawWithNewDeviceId.createOrReplaceTempView("jh_new_incr_records")

    spark.sql(
      s"""
         |select
         |  device_old,
         |  case when (sysver_clean like '1%' and sysver_clean>='10') and token is not null then sha(token) else device_old end as device_token,
         |  muid,
         |  token,
         |  ieid,
         |  mcid,
         |  snid,
         |  oiid,
         |  asid,
         |  sysver_clean as sysver,
         |  factory,
         |  serdatetime
         |from
         |(select device_old,muid,token,ieid,mcid,snid,oaid as oiid,adsid as asid,a.sysver,factory,serdatetime,
         |case when lower(trim(a.sysver)) in ('', '-1', 'unknown', 'null', 'none', 'na', 'other', '未知') or a.sysver is null then 'unknown'
         |  when lower(a.sysver) not rlike '^([0-9a-z]|\\\\.|-| )+$$' then 'unknown'
         |  when a.sysver = b.vernum then b.sysver
         |  when concat(split(regexp_replace(lower(a.sysver), 'android| ', ''), '\\\\.')[0], '.', split(regexp_replace(lower(a.sysver), 'android| ', ''), '\\\\.')[1]) in ('1.0', '1.1', '1.5', '1.6', '2.0', '2.1', '2.2', '2.3', '3.0', '3.1', '3.2', '4.0', '4.1', '4.2', '4.3', '4.4', '4.4w', '5.0', '5.1', '6.0', '7.0', '7.1', '8.0', '8.1', '9.0', '10.0', '11.0') then concat(split(regexp_replace(lower(a.sysver), 'android| ', ''), '\\\\.')[0], '.', split(regexp_replace(lower(a.sysver), 'android| |w', ''), '\\\\.')[1])
         |else 'unknown' end as sysver_clean
         |from jh_new_incr_records a
         |left join
         |(select sysver,vernum from dm_sdk_mapping.sysver_mapping_par where version='1004') b
         |on a.sysver=b.vernum
         |) t
         |""".stripMargin).createOrReplaceTempView("jh_new_incr_records_sysver_etl")

    // jh表新增部分device_token作为其muid和匹配到的部分写入全量表
    spark.sql(
      s"""
         |select
         |  device_old,
         |  device_token,
         |  device_token as muid,
         |  token,
         |  ieid,
         |  mcid,
         |  snid,
         |  oiid,
         |  asid,
         |  sysver,
         |  factory,
         |  serdatetime
         |from jh_new_incr_records_sysver_etl
         |union all
         |select
         |  device_old,
         |  device_token,
         |  muid,
         |  token,
         |  ieid,
         |  mcid,
         |  snid,
         |  oiid,
         |  asid,
         |  sysver,
         |  factory,
         |  serdatetime
         |from jh_matched_records_sysver_etl
         |""".stripMargin).repartition(50).createOrReplaceTempView("result_union_table")

    spark.sql(
      s"""
         |insert overwrite table $DEVICEID_NEW_IDS_MAPPING_INCR partition(day='$day')
         |select
         |  device_old,
         |  device_token,
         |  muid,
         |  token,
         |  ieid,
         |  mcid,
         |  snid,
         |  oiid,
         |  asid,
         |  sysver,
         |  factory,
         |  serdatetime
         |from result_union_table
         |""".stripMargin)
  }
}


