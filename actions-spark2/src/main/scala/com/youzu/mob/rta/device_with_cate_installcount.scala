package com.youzu.mob.rta

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object device_with_cate_installcount {
  def main(args: Array[String]): Unit = {
    // 1. 初始化
    val spark = SparkSession
      .builder()
      .appName("device_with_cate_installcount")
      .enableHiveSupport()
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    //传入日期参数
    val insert_day = args(0)
    val dim_app_pkg_par = args(1)
    val app_category_par = args(2)

    //RTA最终设备池打一、二级标签
    dw_app_cate_mapping_table(spark: SparkSession,app_category_par: String)
    println("dw_app_cate_mapping_table 执行完毕")

    //向RTA设备标签表插入数据（因在装应用包名未清洗，所以需先跟数仓的映射表映射取到清洗后的包名(优先取清洗后的包名，未清洗的则保留
    dw_device_with_cate_table(spark, insert_day, dim_app_pkg_par)
    println("dw_device_with_cate_table 执行完毕")

    //RTA正负向设备分类及聚合对应的二级游戏分类应用数
    // 向RTA正负向设备表插入设备信息及二级游戏分类的应用数
    dw_device_with_cate_installcount_table(spark, insert_day)
    println("dw_device_with_cate_installcount_table 执行完毕")

    //关闭程序
    spark.stop()
  }




  def dw_app_cate_mapping_table(spark: SparkSession,app_category_par: String): Unit ={
    spark.sql(
      s"""
        |insert overwrite table $DW_APP_CATE_MAPPING
        |select apppkg, cate_l1,cate_l2
        |from(
        |  select apppkg, cate_l1,cate_l2,row_number() over(partition by apppkg, cate_l1,cate_l2 order by flag desc) rn
        |  from
        |  (
        |    select apppkg, cate_l1, cate_l2,1 as flag
        |      from $APP_CATEGORY_MAPPING_PAR
        |      where version='$app_category_par'
        |      group by apppkg,cate_l1,cate_l2
        |  union all
        |      select apppkg, cate_l1, cate_l2,2 as flag
        |      from $RTA_GAME_APP_CATEGORY_MAPPING
        |      group by apppkg,cate_l1,cate_l2
        |  ) b
        |) a
        |where rn =1
        |""".stripMargin)
  }

  def dw_device_with_cate_table(spark: SparkSession, insert_day: String, dim_app_pkg_par: String): Unit = {

    spark.sql(
      s"""
         |insert overwrite table $DW_DEVICE_WITH_CATE partition(day='$insert_day')
         |select /*+ BROADCASTJOIN(d) */ ieid, oiid, c.apppkg, d.cate_l1, d.cate_l2
         |from
         |(
         |  select ieid, oiid, coalesce(b.apppkg,a.pkg) as apppkg
         |  from $DW_GAASID_IN_INSTALLID a
         |      left join
         |      (
         |        select pkg, apppkg
         |        from $DIM_APP_PKG_MAPPING_PAR
         |        where version='$dim_app_pkg_par'
         |        group by pkg,apppkg
         |      ) b on a.pkg=b.pkg
         | ) c
         |left join
         |$DW_APP_CATE_MAPPING d
         |on c.apppkg=d.apppkg
         |""".stripMargin
    )
  }

  def  dw_device_with_cate_installcount_table(spark: SparkSession, insert_day: String): Unit = {
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

}
