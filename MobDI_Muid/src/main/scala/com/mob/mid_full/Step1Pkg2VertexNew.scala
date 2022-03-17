package com.mob.mid_full

import org.apache.spark.sql.{SaveMode, SparkSession}

object Step1Pkg2VertexNew {


  def main(args: Array[String]): Unit = {

    val pkgReinstallTimes = args(0).toInt
    val maxOidSize_000 = args(1).toInt
    val maxOidSize = 5000
    val app_Size = args(2).toInt
    val month = args(3)
    val version = args(4)
    val table = args(5)
    val resultTable = args(6)

    val monthFilter =
      if (month.contains("-")) {
        val Array(startMonth, endMonth) = month.split("-")
        s"between $startMonth and $endMonth"
      } else {
        s"=$month"
      }

    // 构造图的边

    val spark = SparkSession.builder().appName(s"Step1Pkg2Vertex__$version")
      .enableHiveSupport()
      .getOrCreate()


    spark.udf.register[Seq[(String, String, Int)], Seq[String]]("openid_resembled", openid_resembled)
    spark.udf.register("get_pkg_ver", getPkgVersion _)

    val rawTable = "dm_mid_master.without_black_duid"

    val tmpSourceTbl = "source_table"
    val source = spark.sql(
      s"""select * from $table
         | where month $monthFilter
         | and unid is not null and pkg_it is not null
         | and duid is not null and trim(duid)<>''
         |""".stripMargin)
    source.createOrReplaceTempView(tmpSourceTbl)

     //过滤出同一个pkg版本安装时间超过$pkgReinstallTimes个的duid
    val blackDuid = spark.sql(
      s"""
         |select * from $tmpSourceTbl a
         |left anti join
         |(
         |  select duid from
         |  (
         |    select duid,get_pkg_ver(pkg_it) pkg_ver,count(*) cnt
         |    from $tmpSourceTbl
         |    group by duid,get_pkg_ver(pkg_it)
         |  ) t where cnt >= $pkgReinstallTimes group by duid
         |)b on a.duid = b.duid
         |""".stripMargin)
    val withoutBlackDuidTmpTbl = "without_black_duid"
    blackDuid.createOrReplaceTempView(withoutBlackDuidTmpTbl)
    blackDuid.write.mode(SaveMode.Overwrite).saveAsTable(rawTable)


    val tableStep1000 = s"${resultTable}_step1_000"
    val tableStep1Non000 = s"${resultTable}_step1_non000"
    val tableStep2000 = s"${resultTable}_step2_000"
    val tableStep2Non000 = s"${resultTable}_step2_non000"

    val tmpStep1000 = s"step1_000"
    val tmpStep1Non000 = s"step1_non000"


    // app同版本相同安装时间(pkg_it)
    /**
     * 过滤出出现过一次以上,maxOidSize次以下的pkg_it
     * 考虑到duid会变,给予极限值变化5000次(假设同一个app在毫秒级安装不重复)
     */
    val pkgItCount000 = spark.sql(
      s"""
         |select pkg_it from (
         |select pkg_it ,count(duid) cc
         |from $withoutBlackDuidTmpTbl where pkg_it like '%000'
         |group by pkg_it,upper(factory),upper(model)
         |having cc > 1 and cc < $maxOidSize_000
         |) ttt
         |""".stripMargin).cache()
    pkgItCount000.createOrReplaceTempView(tmpStep1000)
    pkgItCount000.write.mode(SaveMode.Overwrite).saveAsTable(tableStep1000)

    val pkgItCountNon000 = spark.sql(
          s"""
             |select pkg_it from (
             |select pkg_it ,count(duid) cc
             |from $withoutBlackDuidTmpTbl where pkg_it not like '%000'
             |group by pkg_it,upper(factory),upper(model)
             |having cc > 1 and cc < $maxOidSize
             |) ttt
             |""".stripMargin).cache()
    pkgItCountNon000.createOrReplaceTempView(tmpStep1Non000)
    pkgItCountNon000.write.mode(SaveMode.Overwrite).saveAsTable(tableStep1Non000)

    spark.sql(
      s"""
        |select tmp0.pkg_it,unid from $table tmp0
        |left semi join $tableStep1000 tmp1 on tmp0.pkg_it = tmp1.pkg_it
        |where tmp0.pkg_it like '%000'
        |""".stripMargin).write.mode(SaveMode.Overwrite).saveAsTable(tableStep2000)

    spark.sql(
      s"""
        |select tmp0.pkg_it,unid from $table tmp0
        |left semi join $tableStep1Non000 tmp1 on tmp0.pkg_it = tmp1.pkg_it
        |where tmp0.pkg_it not like '%000'
        |""".stripMargin).write.mode(SaveMode.Overwrite).saveAsTable(tableStep2Non000)

    //pkgItCount000.unpersist()
    //pkgItCountNon000.unpersist()

    /**
     * t1.获取每个上一步过滤后的pkg_it的unid列表
     * t2.把unid列表转换为两两连接的边,((字典序较大的顶点,字典序较小的顶点),1)
     * t3.过滤出出现过不少于app_Size次数的边
     */
    spark.sql(
      s"""
         |insert overwrite table $resultTable partition(month='$month',version='$version')
         |select id1,id2 from (
         |   select tid_info._1 as id1, tid_info._2 as id2 from
         |   (
         |      select openid_resembled(tids) as tid_list from
         |      (
         |        select collect_set(unid) as tids
         |           from
         |           (
         |            select pkg_it,unid from $tableStep2000
         |            union all
         |            select pkg_it,unid from $tableStep2Non000
         |           ) tmp0
         |           group by tmp0.pkg_it
         |       ) t1
         |     ) t2  LATERAL VIEW explode(tid_list) tmp0 AS tid_info
         | ) t3 group by id1,id2 having count(1) >= $app_Size
         |""".stripMargin
    )

    spark.stop()
  }
}
