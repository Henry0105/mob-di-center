package com.mob.mid_full

import org.apache.spark.sql.SparkSession

object Step1Pkg2VertexCount {


  def main(args: Array[String]): Unit = {

    val pkgReinstallTimes = args(0).toInt
    val maxOidSize = args(1).toInt
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

    val tmpSourceTbl = "source_table"
    val source = spark.sql(
      s"""select * from $table
         | where month $monthFilter
         | and unid is not null and pkg_it is not null
         | and duid is not null and trim(duid)<>''
         |""".stripMargin)
    source.createOrReplaceTempView(tmpSourceTbl)

    // app同版本相同安装时间(pkg_it)
    /**
     * 过滤出出现过一次以上,maxOidSize次以下的pkg_it
     */
    val pkgItCount = spark.sql(
      s"""
         |select pkg_it ,count(1) cc
         |from $tmpSourceTbl where pkg_it not like '%000'
         |group by pkg_it having  cc > 1 and cc < $maxOidSize
         |""".stripMargin)
    val normalBehaviorPkg = "normal_behavior_pkg_it"
    pkgItCount.createOrReplaceTempView(normalBehaviorPkg)

    // 过滤出同一个pkg版本安装时间超过$pkgReinstallTimes个的duid
    val blackDuid = spark.sql(
      s"""
         |select duid from
         |(select duid,get_pkg_ver(pkg_it) pkg_ver,count(*) cnt
         |  from $tmpSourceTbl
         |  where pkg_it not like '%000'
         |  group by duid,get_pkg_ver(pkg_it)
         |) a where cnt >= $pkgReinstallTimes group by duid
         |""".stripMargin)

    val blackDuidTmpTbl = "black_duid"
    blackDuid.createOrReplaceTempView(blackDuidTmpTbl)

    /**
     * t1.获取每个上一步过滤后的pkg_it的unid列表
     * t2.把unid列表转换为两两连接的边,((字典序较大的顶点,字典序较小的顶点),1)
     * t3.过滤出出现过不少于app_Size次数的边
     */
    spark.sql(
      s"""
         |insert overwrite table $resultTable partition(month='$month',version='$version')
         |select id1,id2,count(1) cnt from (
         |   select tid_info._1 as id1, tid_info._2 as id2 from
         |   (
         |      select openid_resembled(tids) as tid_list from
         |      (
         |        select collect_set(unid) as tids
         |           from (
         |            select * from $tmpSourceTbl a
         |            left anti join $blackDuidTmpTbl b
         |            on a.duid = b.duid
         |           ) tmp0
         |            join $normalBehaviorPkg tmp1
         |            on tmp0.pkg_it = tmp1.pkg_it
         |            where tmp0.pkg_it not like '%000'
         |           group by tmp0.pkg_it
         |       ) t1
         |     ) t2  LATERAL VIEW explode(tid_list) tmp0 AS tid_info
         | ) t3 group by id1,id2
         |""".stripMargin
    )

    spark.stop()
  }
}
