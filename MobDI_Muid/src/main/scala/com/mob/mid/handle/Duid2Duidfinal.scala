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
