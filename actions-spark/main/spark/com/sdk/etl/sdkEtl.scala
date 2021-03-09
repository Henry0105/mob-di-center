package com.appgo.etl
import org.apache.spark.sql._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.commons.lang.StringUtils
import scala.io.Source
import java.net.URLDecoder
import java.util.regex.Pattern
import org.apache.spark.storage.StorageLevel
import com.mob.sharesdk.utils.Decoders

object sdkEtl {

  case class Config(
    file: String = "",
    input: String = "",
    date: String = "",
    output: String = ""
  )

  val parser = new scopt.OptionParser[Config]("sparkETL") {
    head("sparkETL", "0.2")
    opt[String]('o', "output") text ("output") action {
      (x, c) => c.copy(output = x)
    }
    opt[String]('d', "date") action {
      (x, c) =>
        c.copy(date = x)
    } text ("date")
    opt[String]('f', "file") action {
      (x, c) =>
        c.copy(file = x)
    } text ("sql file")
    arg[String]("input") action {
      (x, c)
      =>
        c.copy(input = x)
    }
  }


  val sc = new org.apache.spark.SparkContext()
  val sqlContext = new org.apache.spark.sql.hive.HiveContext(sc)

  sqlContext.udf.register("decode", (id: String, content: String) =>
    try {
      Pattern.compile("\\s*|\t|\r|\n")
        .matcher(URLDecoder.decode(Decoders.decodeContent(id, content)
        .replace("%7C", ""), "UTF-8"))
        .replaceAll("").replace("\\n", "")
    } catch {
      case _: Throwable => ""
    }
  )

  sqlContext.udf.register("mapping", (keys: String, vals: String) =>
    try {
      val keylist = keys.replace(",", "|").replace(":", "=").split("\t")
      val vallist = StringUtils.splitPreserveAllTokens(vals, "|")
      if (keylist.size == vallist.size) {
        val items = for (kv <- (keylist zip vallist)) yield kv._1 + ":" + kv._2
        items mkString "\001"
      } else if (keylist.size > vallist.size) {
        val n = vallist.size
        val keylist_new = new Array[String](n)
        Array.copy(keylist, 0, keylist_new, 0, n)
        val items = for (kv <- (keylist_new zip vallist)) yield kv._1 + ":" + kv._2
        items mkString "\001"
      } else {
        ""
      }
    } catch {
      case _: Throwable => ""
    })


  def main(args: Array[String]): Unit = {
    val cfg = parser.parse(args, Config()) match {
      case Some(config) => config
      case None => Config()
    }

    val sqlTemplate = (for (line <- Source.fromFile(cfg.file).getLines()) yield line) mkString "\n"
    val sql = sqlTemplate.replace("${DATE}", cfg.date)

    val log = sc.textFile(cfg.input)
    val rdd = log.repartition(1500)
    rdd.persist(StorageLevel.MEMORY_ONLY)


    val schema = StructType(StructField("line", StringType, true) :: Nil)

    val rowRDD = rdd.map(p => Row(p.trim)).distinct()
    val logDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    logDataFrame.registerTempTable("log")

    val results = sqlContext.sql(sql)

    if (cfg.output != "") results.rdd.distinct().saveAsTextFile(cfg.output)

    rdd.unpersist()
    sc.stop()
  }

}

