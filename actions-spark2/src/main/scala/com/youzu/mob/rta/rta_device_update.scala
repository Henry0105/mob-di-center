package com.youzu.mob.rta

import org.apache.spark.sql.SparkSession
import com.youzu.mob.utils.Constants._

/**
 * @author linke
 * @create 2022-06-13-10:25
 * @describe RTA标签数据刷新任务
 */
object rta_device_update {

  def main(args: Array[String]): Unit = {
    // 1. 初始化
    val spark = SparkSession
      .builder()
      .appName("rta_data_update")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //传入日期参数
    val insert_day = args(0)
    val full_par = args(1)
    val dim_app_pkg_par = args(2)
    val app_category_par = args(3)

    //--在装应用汇总表
    //--插入30天内包名不为空且ieid/oiid任一不为空的在装应数据
    dw_install_app_all_rta_table(spark, insert_day)
   println("step1: dw_install_app_all_rta_table 执行完毕")

    //RTA设备池_未添加分类字段
    //从dm_mobdi_master.dwd_gaas_rta_id_data_di中提取出1周内的去重ieid、oiid数据，入结果表
    dw_gaas_id_data_di_rta_table(spark)
    println("step2: dw_gaas_id_data_di_rta_table 执行完毕")

    //更新RTA异常设备池(对应oiid数>2的imei，及对应ieid数>3的oiid均入黑名单，需继承历史黑名单)
    dw_gaasid_blacklist_table(spark,insert_day,full_par)
    println("step3: dw_gaasid_blacklist_table 执行完毕")

    //RTA设备信息池(RTA请求设备信息去重表过滤黑名单，不继承历史数据)
    dw_gaasid_final_table(spark, insert_day)
    println("step4: dw_gaasid_final_table 执行完毕")

    //RTA设备池与在装应用清洗表取交集
    dw_gaasid_in_installid_table(spark,insert_day)
    println("step5: dw_gaasid_in_installid_table 执行完毕")

    //RTA最终设备池打一、二级标签
    //向RTA设备标签表插入数据（因在装应用包名未清洗，所以需先跟数仓的映射表映射取到清洗后的包名(优先取清洗后的包名，未清洗的则保留
    dw_device_with_cate_table(spark,insert_day,dim_app_pkg_par,app_category_par)
    println("step6: dw_device_with_cate_table 执行完毕")

    //RTA正负向设备分类及聚合对应的二级游戏分类应用数
    // 向RTA正负向设备表插入设备信息及二级游戏分类的应用数
    dw_device_with_cate_installcount_table(spark,insert_day)
    println("step7: dw_device_with_cate_installcount_table 执行完毕")

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

    //新建0325临时结果表，包括付费天数及距今付费间隔时间维度的设备
    dm_device_rec_for_0325_pre_table(spark)
    println("step11: dm_device_rec_for_0325_pre_table 执行完毕")

    //更新至0325规则结果表最新分区，增加增改、删、不变的状态
    dm_device_rec_for_0325_table(spark,insert_day,full_par)
    println("step12: dm_device_rec_for_0325_table 执行完毕")

    //新建临时表，包括本次所有0326推荐设备数据
    dm_device_rec_for_0326_pre_table(spark,insert_day)
    println("step13: dm_device_rec_for_0326_pre_table 执行完毕")

    //新建临时表，包括本次所有0326推荐设备数据+pre表中没有但在0326表次新分区中有，且未发回安装数据且不在黑名单的设备
    dm_device_rec_for_0326_pre_second_table(spark,insert_day,full_par)
    println("step14: dm_device_rec_for_0326_pre_second_table 执行完毕")

    //更新至0326规则结果表最新分区，增加增改、删、不变的状态
    dm_device_rec_for_0326_table(spark,insert_day,full_par)
    println("step15: dm_device_rec_for_0326_table 执行完毕")

    //rta历史请求时存疑的ieid,oiid
    dw_gaasid_doubtful_table(spark,insert_day,full_par)
    println("step16: dw_gaasid_doubtful_table 执行完毕")

    //新建临时表，包括本次所有0327推荐设备数据
    dm_device_rec_for_0327_pre_table(spark)
    println("step17: dm_device_rec_for_0327_pre_table 执行完毕")

    //更新至0327规则结果表最新分区，增加增改、删、不变的状态
    dm_device_rec_for_0327_table(spark,insert_day,full_par)
    println("step18: dm_device_rec_for_0327_table 执行完毕")

    //汇总规则数据(0.5.1后的版本使用)
    dm_device_rec_for_all_table(spark,insert_day)
    println("step19: dm_device_rec_for_all_table 执行完毕")

    //关闭程序
    spark.stop()
  }

  def dw_install_app_all_rta_table(spark: SparkSession, insert_day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_INSTALL_APP_ALL_RTA partition(day='$insert_day')
         |  select
         |  ieid,
         |  oiid,
         |  pkg
         |from
         |    $DWD_LOG_DEVICE_INSTALL_APP_ALL_INFO_SEC_DI
         |where day>=date_format(date_sub(current_date,31),'yyyyMMdd')
         |    and pkg is not null and trim(pkg)!='' and substr(firstinstalltime,-3)!='000'
         |    and ((ieid is not null and trim(ieid)!='') or (oiid is not null and trim(oiid)!=''))
         |group by ieid,oiid,pkg
         |""".stripMargin)
  }

  def dw_gaas_id_data_di_rta_table(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_GAAS_ID_DATA_DI_RTA
         |select
         |  ieid,
         |  oiid
         |from $DWD_GAAS_RTA_ID_DATA_DI
         |where day >=date_format(date_sub(current_date,8),'yyyyMMdd')
         |group by ieid,oiid
         |""".stripMargin)
  }


  def dw_gaasid_blacklist_table(spark: SparkSession, insert_day: String,full_par: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_GAASID_BLACKLIST partition(day='$insert_day')
         |   select
         |    'ieid' as type,
         |    ieid as idvalue
         |    from $DW_GAAS_ID_DATA_DI_RTA
         |    where ieid is not null and trim(ieid)!='' and oiid is not null and trim(oiid)!=''
         |    group by ieid
         |    having count(1)>2
         |union
         |   select 'oiid' as type, oiid as idvalue
         |    from $DW_GAAS_ID_DATA_DI_RTA
         |    where oiid is not null and trim(oiid)!='' and ieid is not null and trim(ieid)!=''
         |    group by oiid
         |    having count(1)>3
         |union
         |   select type, idvalue
         |    from $DW_GAASID_BLACKLIST
         |    where day='$full_par'
         |""".stripMargin
    )
  }

  def dw_gaasid_final_table(spark: SparkSession, insert_day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_GAASID_FINAL
         |select
         |distinct
         |if(ieid_black is null,ieid,'') as ieid,
         |if(oiid_black is null,oiid,'') as oiid
         |from
         |  (
         |  select a.ieid as ieid, a.oiid as oiid, b.ieid as ieid_black, c.oiid as oiid_black
         |  from $DW_GAAS_ID_DATA_DI_RTA a
         |      left join
         |        (
         |          select idvalue as ieid
         |          from $DW_GAASID_BLACKLIST
         |          where type='ieid' and day='$insert_day'
         |          group by idvalue
         |        ) b on a.ieid=b.ieid
         |      left join
         |        (
         |          select idvalue as oiid
         |          from $DW_GAASID_BLACKLIST
         |          where type='oiid' and day='$insert_day'
         |          group by idvalue
         |        ) c on a.oiid=c.oiid
         |  )d
         |""".stripMargin
    )
  }

  def dw_gaasid_in_installid_table(spark: SparkSession, insert_day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_GAASID_IN_INSTALLID
         |    select
         |    a.ieid,
         |    a.oiid,
         |    b.pkg
         |    from $DW_GAASID_FINAL a
         |        join
         |        (
         |          select ieid, pkg
         |          from $DW_INSTALL_APP_ALL_RTA
         |          where day='$insert_day'and ieid is not null and trim(ieid)!=''
         |          group by ieid,pkg
         |         ) b on a.ieid=b.ieid
         |    group by a.ieid,a.oiid,b.pkg
         |union
         |    select a.ieid, a.oiid, b.pkg
         |    from $DW_GAASID_FINAL a
         |        join
         |        (
         |          select oiid, pkg
         |          from $DW_INSTALL_APP_ALL_RTA
         |          where day='$insert_day'and oiid is not null and trim(oiid)!=''
         |          group by oiid,pkg
         |         ) b on a.oiid=b.oiid
         |    group by a.ieid,a.oiid,b.pkg
         |""".stripMargin
    )
  }

  def dw_device_with_cate_table(spark: SparkSession, insert_day: String,dim_app_pkg_par: String,app_category_par: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_DEVICE_WITH_CATE partition(day='$insert_day')
         |select
         |ieid,
         |oiid,
         |c.apppkg,
         |d.cate_l1,
         |d.cate_l2
         |from
         |    (
         |      select ieid, oiid, coalesce(b.apppkg,a.pkg) as apppkg
         |      from $DW_GAASID_IN_INSTALLID a
         |          left join
         |          (
         |            select pkg, apppkg
         |            from $DIM_APP_PKG_MAPPING_PAR
         |            where version='$dim_app_pkg_par'
         |            group by pkg,apppkg
         |          ) b on a.pkg=b.pkg
         |     ) c
         |      left join
         |      (
         |      select apppkg, cate_l1, cate_l2
         |        from
         |        (
         |        select apppkg, cate_l1, cate_l2, row_number() over(partition by apppkg order by cate_l2) as num
         |            from
         |             (
         |              select apppkg, cate_l1, cate_l2
         |                from $APP_CATEGORY_MAPPING_PAR
         |                where version='$app_category_par'
         |                group by apppkg,cate_l1,cate_l2
         |             union
         |                select apppkg, cate_l1, cate_l2
         |                from $RTA_GAME_APP_CATEGORY_MAPPING
         |                group by apppkg,cate_l1,cate_l2
         |              )
         |         )
         |         where num<2) d on c.apppkg=d.apppkg
         |""".stripMargin
    )
  }

  def dw_device_with_cate_installcount_table(spark: SparkSession, insert_day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_DEVICE_WITH_CATE_INSTALLCOUNT partition(day='$insert_day')
         |  select
         |    'ieid' as idtype,
         |    ieid as idvalue,
         |    type,
         |    cate_l2_clean,
         |    count(1) as appnum
         |  from
         |      (
         |        select ieid, case when cate_l1_clean like '%游戏%' then 1 else 0 end as type, cate_l2_clean
         |        from $DW_DEVICE_WITH_CATE
         |        where day='$insert_day' and ieid is not null and trim(ieid)!=''
         |      )a
         |      group by type,ieid,cate_l2_clean
         |union
         |    select
         |    'oiid' as idtype,
         |    oiid as idvalue,
         |    type,
         |    cate_l2_clean,
         |    count(1) as appnum
         |    from
         |      (
         |        select oiid, case when cate_l1_clean like '%游戏%' then 1 else 0 end as type, cate_l2_clean
         |        from $DW_DEVICE_WITH_CATE
         |        where day='$insert_day' and oiid is not null and trim(oiid)!=''
         |       ) a
         |      group by type,oiid,cate_l2_clean
         |""".stripMargin
    )
  }

  def dm_device_rec_for_0324_pre_table(spark: SparkSession, insert_day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0324_PRE
         |   select
         |    '0324' as code,
         |    idtype,
         |    idvalue,
         |    0.8 as recommend,
         |    1 as status
         |    from $DW_DEVICE_WITH_CATE_INSTALLCOUNT
         |    where day='$insert_day' and type=1
         |    group by idtype,idvalue
         |union
         |   select
         |    '0324' as code,
         |    a.idtype,
         |    a.idvalue,
         |    0.1 as recommend,
         |    1 as status
         |    from
         |      (
         |        select idtype, idvalue
         |         from $DW_DEVICE_WITH_CATE_INSTALLCOUNT
         |         where day='$insert_day' and type!=1
         |         group by idtype,idvalue
         |      ) a
         |      left join
         |      (
         |       select idtype, idvalue
         |       from $DW_DEVICE_WITH_CATE_INSTALLCOUNT
         |       where day='$insert_day' and type=1
         |       group by idtype,idvalue
         |      ) b
         |       on a.idtype=b.idtype and a.idvalue=b.idvalue
         |    where b.idvalue is null
         |""".stripMargin
    )
  }

  def dm_device_rec_for_0324_pre_second_table(spark: SparkSession, insert_day: String,full_par: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0324_PRE_SECOND
         |  select
         |  code,
         |  idtype,
         |  idvalue,
         |  recommend,
         |  status
         |    from $DM_DEVICE_REC_FOR_0324_PRE
         |  union
         |   select
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



  def dm_device_rec_for_0324_table(spark: SparkSession, insert_day: String,full_par: String): Unit = {
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

  def dm_device_rec_for_0325_table(spark: SparkSession, insert_day: String,full_par: String): Unit = {
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
         |      select a.idtype,a.idvalue,a.appnum,b.code_id as code
         |      from
         |        (
         |        select idtype,idvalue,appnum,cate_l2_clean from $DW_DEVICE_WITH_CATE_INSTALLCOUNT where day='$insert_day' and type=1
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

  def dw_gaasid_doubtful_table(spark: SparkSession, insert_day: String,full_par: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DW_GAASID_DOUBTFUL partition(day='$insert_day')
         |select
         |  'ieid' as type,
         |   ieid as idvalue
         |from $DWD_GAAS_RTA_ID_DATA_DI
         |  where day >= date_format(date_sub(current_date, 8), 'yyyyMMdd')
         |      and imei_md5_doubtful = '1'
         |  group by ieid
         |union
         |  select
         |    'oiid' as type,
         |     oiid as idvalue
         |  from $DWD_GAAS_RTA_ID_DATA_DI
         |  where day >= date_format(date_sub(current_date, 8), 'yyyyMMdd')
         |    and oaid_md5_doubtful = '1'
         |  group by oiid
         |union
         |  select
         |    type,idvalue
         |  from $DW_GAASID_DOUBTFUL
         |  where day ='$full_par'
         |""".stripMargin
    )
  }

  def dm_device_rec_for_0327_pre_table(spark: SparkSession): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0327_PRE
         |    select '03271' as code,
         |        a.type as idtype,
         |        idvalue,
         |        0 as recommend,
         |        1 as status
         |    from $DW_GAASID_BLACKLIST a left anti
         |        join $DW_GAASID_DOUBTFUL b
         |      on a.type = b.type and a.idvalue = b.idvalue
         |union
         |    select '03272' as code,
         |        a.type as idtype,
         |        a.idvalue,
         |        0 as recommend,
         |        1 as status
         |    from $DW_GAASID_BLACKLIST a
         |        join $DW_GAASID_DOUBTFUL b
         |     on a.type = b.type and a.idvalue = b.idvalue
         |union
         |    select '03273' as code,
         |        a.type as idtype,
         |        a.idvalue,
         |        0 as recommend,
         |        1 as status
         |    from $DW_GAASID_DOUBTFUL a left anti
         |        join $DW_GAASID_BLACKLIST b
         |     on a.type = b.type and a.idvalue = b.idvalue
         |""".stripMargin
    )
  }

  def dm_device_rec_for_0327_table(spark: SparkSession, insert_day: String,full_par: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_0327 partition(day='$insert_day')
         |   select
         |     coalesce(a.code, b.code) as code,
         |     coalesce(a.idtype, b.idtype) as idtype,
         |     coalesce(a.idvalue, b.idvalue) as idvalue,
         |     coalesce(a.recommend, b.recommend) as recommend,
         |     case when a.recommend = b.recommend then 2 when a.recommend is not null
         |     and b.recommend is null then 1 when a.recommend is not null
         |     and b.recommend is not null
         |     and a.recommend != b.recommend then 1 else 0 end as status
         |   from
         |     $DM_DEVICE_REC_FOR_0327_PRE a
         | full join
         |  (
         |    select code,idtype,idvalue,recommend
         |    from $DM_DEVICE_REC_FOR_0327
         |    where day = '$full_par'
         |  ) b
         |   on a.code = b.code and a.idtype = b.idtype and a.idvalue = b.idvalue
         |""".stripMargin
    )
  }

  def dm_device_rec_for_all_table(spark: SparkSession, insert_day: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table $DM_DEVICE_REC_FOR_ALL partition(day='$insert_day')
         |  select
         |    code,
         |    idtype,
         |    idvalue,
         |    recommend,
         |    status
         |    from $DM_DEVICE_REC_FOR_0324
         |    where day='$insert_day'
         |  union
         |    select
         |      code, idtype, idvalue, recommend, status
         |      from $DM_DEVICE_REC_FOR_0325
         |      where day='$insert_day'
         |  union
         |    select
         |      code, idtype, idvalue, recommend, status
         |      from $DM_DEVICE_REC_FOR_0326
         |      where day='$insert_day'
         |  union
         |    select
         |      code, idtype, idvalue, recommend, status
         |      from $DM_DEVICE_REC_FOR_0327
         |      where day='$insert_day'
         |""".stripMargin
    )
  }

}
