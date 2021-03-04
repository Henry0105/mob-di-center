package com.youzu.mob.profile.tags_info_full

import java.time.format.DateTimeFormatter
import java.util.Locale

import com.youzu.mob.profile.Logging
import com.youzu.mob.profile.tags_info_full.beans.{ProfileInfo, QueryUnitContext, TagsCheckerParam}
import com.youzu.mob.profile.tags_info_full.handle.QueryUnitFactory
import com.youzu.mob.profile.tags_info_full.helper.{MetaDataHelper, TablePartitionsManager}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

/**
 * @author xlmeng
 */
object TagsChecker {

  val fmt: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd", Locale.CHINA)

  def main(args: Array[String]): Unit = {
    val defaultParam: TagsCheckerParam = TagsCheckerParam()
    val projectName = "TagsChecker"
    val parser = new OptionParser[TagsCheckerParam](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .required()
        .action((x, c) => c.copy(day = x))

      opt[Boolean]('t', "test")
        .text(s"测试")
        .action((x, c) => c.copy(test = x))

      opt[String]('e', "exclude")
        .text(s"需要排除计算的表")
        .action((x, c) => c.copy(exclude = x))
    }

    parser.parse(args, defaultParam) match {
      case Some(p) =>
        println(s"参数为:$p")
        val spark = SparkSession
          .builder()
          .appName(this.getClass.getSimpleName + p.day)
          .enableHiveSupport()
          .getOrCreate()

        TagsChecker(spark, p).run()
        spark.close()
      case _ =>
        println(s"参数有误，参数为: ${args.mkString(",")}")
        sys.exit(1)
    }
  }
}

case class TagsChecker(@transient spark: SparkSession, p: TagsCheckerParam) extends Logging {

  val pre: String = if (p.test) ".pre" else ""

  def run(): Unit = {

    // 取出需要检查的表和频率信息
    val tbManager = TablePartitionsManager(spark)

    val tagsProfiles: Array[ProfileInfo] = MetaDataHelper(spark, pre).getComputedProfiles(p.day, p.exclude)
    val confidenceProfiles: Array[ProfileInfo] = MetaDataHelper(spark, pre).getProfileConfidence()

    val cxt: QueryUnitContext = QueryUnitContext(spark, p.day, tbManager)
    val checkRes = check(cxt, tagsProfiles).union(check(cxt, confidenceProfiles, isConfidence = true))

    if (!checkRes.forall(_._2)) {
      val errorSet = checkRes.filterNot(_._2).groupBy(e => s"${e._1}/${e._3}").keySet
      errorSet.foreach(errMsg => logger.error(s"Error: 有表分区未生成 => $errMsg"))

      val errorPartSet = checkRes.filterNot(_._2).groupBy(_._1).keySet
      errorPartSet.foreach(errPart => logger.error(s"Error: 表未生成 => $errPart"))
      spark.close()
      sys.exit(1)
    }

  }

  private def check(cxt: QueryUnitContext, profiles: Array[ProfileInfo], isConfidence: Boolean = false):
  Seq[(String, Boolean, String)] = {
    val queryUnits = QueryUnitFactory.createQueryUnit(cxt, profiles, isConfidence)
    queryUnits.map(_.check())
  }

}

