package com.mob.mid_full_v2

import com.mob.mid_full.openid_resembled
import org.apache.spark.sql.SparkSession

object MidStep1Pkg2VertexRnid {
  /**
    *
    * 1.创建表，rnid_unid_mid 映射表
    * use mobdi_test;
    * create table rnid_unid_mid(rid string，unid Long,mid Long);
    *
    */
  def main(args: Array[String]): Unit = {

//    val pkgReinstallTimes = args(0).toInt
    val maxOidSize = args(1).toInt
    val app_Size = 7
    val month = args(0)
    val version = args(2)
//    val table = args(5)
    val resultTable = args(3)


    //表名
    val tmpSourceTbl="dm_mid_master.pkg_it_duid_category_tmp_rid"
    val rnidRnidBlacklist="dm_mid_master.rnid_ieid_blacklist"


    val spark = SparkSession.builder().appName(s"Step1Pkg2Vertex__$version")
      .enableHiveSupport()
      .getOrCreate()


    spark.udf.register[Seq[(String, String, Int)], Seq[String]]("openid_resembled", openid_resembled)


    // app同版本相同安装时间(pkg_it)
    /**
      * 过滤出出现过一次以上,maxOidSize次以下的pkg_it
      * 考虑到duid会变,给予极限值变化1000次(假设同一个app在毫秒级安装不重复)
      */
    val pkgItCount = spark.sql(
      s"""
         |select concat_ws('',pkg,version,firstinstalltime) pkg_it ,count(distinct rnid) cc
         |from $tmpSourceTbl where firstinstalltime not like '%000'
         |group by concat_ws('',pkg,version,firstinstalltime)  having  cc > 1 and cc < $maxOidSize
         |""".stripMargin)
    val normalBehaviorPkg = "normal_behavior_pkg_it"
    pkgItCount.createOrReplaceTempView(normalBehaviorPkg)


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
         |           from (
         |            select rnid, concat_ws('',pkg,version,firstinstalltime) pkg_it,unid from $tmpSourceTbl a
         |            left anti join $rnidRnidBlacklist b
         |            on a.rnid = b.rnid and a.rnid <> '' and a.rnid is not null
         |           )  tmp0
         |            join $normalBehaviorPkg tmp1
         |            on tmp0.pkg_it = tmp1.pkg_it
         |            where tmp0.pkg_it not like '%000'
         |           group by tmp0.pkg_it
         |       ) t1
         |     ) t2  LATERAL VIEW explode(tid_list) tmp0 AS tid_info
         | ) t3 group by id1,id2 having count(1) >= $app_Size
         |""".stripMargin
    )

    spark.stop()
  }

}
