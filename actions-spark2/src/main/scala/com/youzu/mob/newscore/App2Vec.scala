package com.youzu.mob.newscore

import com.youzu.mob.stall.initial
import org.apache.spark.sql.SparkSession
import scopt.OptionParser

import scala.collection.mutable.ArrayBuffer

//入参样例类
case class param(
                  inputTable: String = "",
                  outputTable: String = "",
                  day: String = "",
                  sql: String = "",
                  flag: String = "",
                )

class App2Vec {

  def run(spark: SparkSession, param: param) = {

    //1.拆分数据，存成11份sql
    val sqls = ArrayBuffer[String]()

    for (i <- 0 to 10) {
      var limit = i / 10.toDouble
      val seedSql =
        s"""
           |WITH seed AS
           |(
           |  SELECT *
           |  FROM ${param.inputTable}
           |  WHERE day = '${param.day}'
           |  AND round(conv(substr(device, 0, 4), 16 ,10)/65535,1) = '$limit'
           |)
           |
           |INSERT OVERWRITE TABLE ${param.outputTable} PARTITION(day=${param.day},stage=$limit)
           |""".stripMargin

      val dosql = seedSql + param.sql
      sqls += dosql
    }

    //2.并行执行11段sql
    sqls.foreach(print(_))
    sqls.par.foreach(x => spark.sql(x))

    spark.stop()

  }

}

object App2Vec {
  def main(args: Array[String]): Unit = {

    val defaultParams = param()
    val parser = new OptionParser[param]("App2vec") {
      head("app2vec")
      opt[String]("inputTable")
        .required()
        .text(s"输入表")
        .action((x, c) => {
          assert(x != null, "--inputTable不能传入空")
          c.copy(inputTable = x)
        })
      opt[String]("outputTable")
        .required()
        .text(s"输出表")
        .action((x, c) => {
          assert(x != null, "--outputTable不能传入空")
          c.copy(outputTable = x)
        })
      opt[String]('d', "day")
        .required()
        .text(s"执行日期")
        .action((x, c) => {
          assert(x.matches("[0-9]{8}"), s"-d或者--day只接受yyyyMMdd类型的日期，不能解析：$x")
          c.copy(day = x)
        })
      opt[String]("sql")
        .required()
        .text(s"逻辑sql")
        .action((x, c) => {
          assert(x != null, "--sql不能传入空")
          c.copy(sql = x)
        })
      opt[String]("flag")
        .required()
        .text(s"标识")
        .action((x, c) => c.copy(flag = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(param) =>

        val spark = initial(s"app2vec_${param.day}_flag")
        val vec = new App2Vec
        vec.run(spark, param)

      case _ => println(s"参数有误:${args.mkString(",")}")
        sys.exit(1)
    }
  }
}
