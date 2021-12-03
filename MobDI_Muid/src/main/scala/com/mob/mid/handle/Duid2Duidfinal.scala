package com.mob.mid.handle

import com.mob.mid.bean.Param
import com.mob.mid.helper.{GraphHelper, Pkg2VertexHelper}
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

object Duid2Duidfinal {

  def main(args: Array[String]): Unit = {

    val defautParm = Param()

    val parser: OptionParser[Param] = new OptionParser[Param]("Duid2Duidfinal") {
      head("Duid2Duidfinal")
      opt[String]('d', "day")
        .required()
        .text(s"执行日期")
        .action((x, c) => {
          assert(x.matches("[0-9]{8}"), s"-d或者-day只接受yyyyMMdd格式日期，输入有误：$x")
          c.copy(day = x)
        })
      opt[String]("pday")
        .required()
        .text(s"执行日期前一天")
        .action((x, c) => {
          assert(x.matches("[0-9]{8}"), s"-d或者-day只接受yyyyMMdd格式日期，输入有误：$x")
          c.copy(pday = x)
        })
      opt[String]('i', "inputTable")
        .required()
        .text(s"输入表")
        .action((x, c) => {
          assert(!x.isEmpty, s"-i或者-inputTable输入不能为空")
          c.copy(inputTable = x)
        })
      opt[String]("duidUnidTable")
        .required()
        .text(s"duid-unid全量映射表")
        .action((x, c) => {
          assert(!x.isEmpty, s"-duidUnidTable输入不能为空")
          c.copy(duidUnidTable = x)
        })
      opt[String]("unidFinalTable")
        .required()
        .text(s"unid-unidfinal映射表")
        .action((x, c) => {
          assert(!x.isEmpty, s"-unidFinalTable输入不能为空")
          c.copy(unidFinalTable = x)
        })
      opt[String]("unidMonthTable")
        .required()
        .text(s"duid月表")
        .action((x, c) => {
          assert(!x.isEmpty, s"-unidFinalTable输入不能为空")
          c.copy(unidMonthTable = x)
        })
      opt[String]("vertexTable")
        .required()
        .text(s"每日unid做边")
        .action((x, c) => {
          assert(!x.isEmpty, s"-vertexTable输入不能为空")
          c.copy(vertexTable = x)
        })
      opt[String]("outputTable")
        .required()
        .text(s"输出表")
        .action((x, c) => {
          assert(!x.isEmpty, s"-unidFinalTable输入不能为空")
          c.copy(outputTable = x)
        })
      opt[Int]('p', "pkgItLimit")
        .required()
        .text(s"pkg_it的最大duid数")
        .action((x, c) => {
          assert(x >= 0, s"-p或者-pkgItLimit一定要大于0，输入有误：$x")
          c.copy(pkgItLimit = x)
        })
      opt[Int]('r', "pkgReinstallTimes")
        .required()
        .text(s"version最大安装duid数量")
        .action((x, c) => {
          assert(x >= 0, s"-r或者-pkgReinstallTimes一定要大于0，输入有误：$x")
          c.copy(pkgReinstallTimes = x)
        })
      opt[Int]('e', "edgeLimit")
        .required()
        .text(s"通过pkg_it构造边的最小限制")
        .action((x, c) => {
          assert(x >= 0, s"-e或者-edgeLimit一定要大于0，输入有误：$x")
          c.copy(edgeLimit = x)
        })
      opt[Int]('g', "graphConnectTimes")
        .required()
        .text(s"执行日期")
        .action((x, c) => {
          assert(x >= 0, s"-g或者-graphConnectTimes 一定要大于0，输入有误：$x")
          c.copy(graphConnectTimes = x)
        })
    }

    parser.parse(args, defautParm) match {
      case Some(defaultParams) => {

        val spark: SparkSession = SparkSession
          .builder()
          .appName(s"Duid2Duidfinal_${defautParm.day}")
          .config("hive.exec.dynamic.partition", "true")
          .config("hive.exec.dynamic.partition.mode", "nonstrict")
          .enableHiveSupport()
          .getOrCreate()

        new Duid2Duidfinal(spark, defaultParams).run()

        spark.stop()
      }

      case _ =>
        println(s"参数有误:${args.mkString(",")}")
        sys.exit(1)
    }
  }

}

class Duid2Duidfinal(spark: SparkSession, defaultParam: Param) {

  def run(): Unit = {

    //step1：增量数据根据pkgit构造图的边
    Pkg2VertexHelper.makeEdge(spark, defaultParam)

    //step2：图计算
    GraphHelper.compute(spark, defaultParam)

    spark.stop()

  }
}
