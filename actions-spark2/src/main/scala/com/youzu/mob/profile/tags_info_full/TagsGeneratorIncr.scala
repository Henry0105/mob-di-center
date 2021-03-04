package com.youzu.mob.profile.tags_info_full


import com.youzu.mob.profile.tags_info_full.beans.{ProfileInfo, QueryUnitContext, TagsGeneratorIncrParam}
import com.youzu.mob.profile.tags_info_full.handle.QueryUnitFactory
import com.youzu.mob.profile.tags_info_full.helper.{
  MetaDataHelper, TablePartitionsManager, TagsDateProcess,
  TagsGeneratorHelper
}
import com.youzu.mob.profile.tags_info_full.helper.TagsGeneratorHelper._
import com.youzu.mob.utils.{DateUtils, PropUtils}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.ShutdownHookUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import scopt.OptionParser


object TagsGeneratorIncr {
  val tagsTable: String = "tags_table"
  val confidenceTable: String = "confidence_table"
  var resDataBase: String = _
  var monthUpdateTable: String = _
  var resTable: String = _

  def main(args: Array[String]): Unit = {
    val defaultParams = TagsGeneratorIncrParam()
    val projectName = s"TagsGenerator[${DateUtils.getCurrentDay()}]"
    val parser = new OptionParser[TagsGeneratorIncrParam](projectName) {
      head(s"$projectName")
      opt[String]('d', "day")
        .required()
        .text(s"tag更新时间")
        .action((x, c) => c.copy(day = x))
      opt[Boolean]('s', "sample")
        .optional()
        .text(s"数据是否采样")
        .action((x, c) => c.copy(sample = x))
      opt[Int]('b', "batch")
        .optional()
        .text(s"多少个分区做一次checkpoint")
        .action((x, c) => c.copy(batch = x))
      opt[Boolean]('t', "test")
        .text(s"测试")
        .action((x, c) => c.copy(test = x))
      opt[Boolean]('f', "full")
        .optional()
        .text(s"多少个分区做一次checkpoint")
        .action((x, c) => c.copy(full = x))
      opt[String]('e', "exclude")
        .optional()
        .text(s"需要排除计算的表")
        .action((x, c) => c.copy(exclude = x))
    }

    parser.parse(args, defaultParams) match {
      case Some(p) =>
        println(p)
        val spark: SparkSession = SparkSession
          .builder()
          .enableHiveSupport()
          .getOrCreate()

        resDataBase = if (p.test) "dataengine_tmp_test" else "rp_mobdi_app"
        monthUpdateTable = s"$resDataBase.profile_tags_info_di_month_update"
        resTable = s"$resDataBase.profile_all_tags_info_di"
        if (p.full) {
          new TagsGeneratorIncr(spark, p).run()
        } else {
          new TagsGeneratorIncr(spark, p).run()
        }
        spark.close()
      case _ =>
        println(s"参数有误:${args.mkString(",")}")
        sys.exit(1)
    }
  }

}

class TagsGeneratorIncr(@transient spark: SparkSession, p: TagsGeneratorIncrParam) extends Serializable {

  import TagsGeneratorIncr._

  val pre: String = if (p.test) ".pre" else ""
  var confidenceId2ValueMap: String = _
  var metaHelper: MetaDataHelper = _

  def run(): Unit = {
    // 去mysql读取标签的元数据
    prepare()
    // 查询到tags和confidence信息
    handle()
    // 持久化到hive
    persist2Hive()
    // 月更新最新分区的持久化
    end()
  }

  def prepare(): Unit = {
    setCheckPoint()
    metaHelper = new MetaDataHelper(spark, pre)
    TagsGeneratorHelper.registerNum2Str(spark)
  }

  /**
   * 工厂类去创建sql组成单元，组成执行的sql，执行sql得出结果
   */
  def query(profileInfos: Array[ProfileInfo], cxt: QueryUnitContext, isConfidence: Boolean = false): DataFrame = {
    val queryUnits = QueryUnitFactory.createQueryUnit(cxt, profileInfos, isConfidence)
    val dfArr = queryUnits.map(queryUnit => sql(spark, queryUnit.query()))

    dfArr.sliding(p.batch, p.batch).map(iter => iter.reduce(_ union _).checkpoint())
      .toArray.reduce(_ union _)
  }

  def handle(): Unit = {
    val tbManager: TablePartitionsManager = TablePartitionsManager(spark)
    val cxt = QueryUnitContext(spark, p.day, tbManager, p.sample, p.full)

    handleTags(cxt: QueryUnitContext)
    handleConfidence(cxt: QueryUnitContext)
  }

  def handleTags(cxt: QueryUnitContext): Unit = {
    val profileInfos: Array[ProfileInfo] = metaHelper.getComputedProfiles(p.day, p.exclude, p.full)
    println("profileInfos nums:" + profileInfos.length)
    val tagsDF = query(profileInfos, cxt)
    tagsDF.createOrReplaceTempView(tagsTable)
  }

  def handleConfidence(cxt: QueryUnitContext): Unit = {
    val confidenceInfos = metaHelper.getProfileConfidence()
    println("confidenceInfos nums:" + confidenceInfos.length)
    val confidenceDF = query(confidenceInfos, cxt, isConfidence = true)
    confidenceDF.createOrReplaceTempView(confidenceTable)
  }

  def persist2Hive(): Unit = {
    val tagListProfileIdSet: Set[String] = metaHelper.findTaglistLikeProfiles("tag_list;").values.toSet
    val cateListProfileIdSet: Set[String] = metaHelper.findTaglistLikeProfiles("catelist;").values.toSet
    val tagListProfileIdSetBC: Broadcast[Set[String]] = spark.sparkContext.broadcast(tagListProfileIdSet)
    val cateListProfileIdSetBC: Broadcast[Set[String]] = spark.sparkContext.broadcast(cateListProfileIdSet)
    // 去掉没有标签值的标签的置信度
    spark.udf.register("kv_str_to_map", new TagsGeneratorHelper.kvStr2map)
    spark.udf.register("remove_old_tag_list", TagsGeneratorHelper.removeOldTagsLike(tagListProfileIdSetBC) _)
    spark.udf.register("remove_old_tag_catelist", TagsGeneratorHelper.removeOldTagsLike(cateListProfileIdSetBC) _)

    sql(spark,
      s"""
         |INSERT OVERWRITE TABLE $resTable PARTITION (day='${p.day}')
         |SELECT device, tags, confidence, tags_like_sign
         |FROM   (
         |        SELECT device
         |             , kv_str_to_map(kv, '$pairSep', '$kvSep', update_time, '${p.day}') as tags
         |             , kv_str_to_map(confidence, '$pairSep', '$kvSep', update_time, '${p.day}') as confidence
         |             , max(tags_like_sign) as tags_like_sign
         |        FROM (
         |              SELECT device, kv, null AS confidence, update_time, tags_like_sign
         |              FROM   $tagsTable
         |              UNION ALL
         |              SELECT device, null AS kv, confidence, update_time, tags_like_sign
         |              FROM   $confidenceTable
         |             ) res_1
         |        GROUP BY device
         |       ) res_2
         |WHERE  tags is not null or confidence is not null
         |""".stripMargin)

    println("标签增量表写入成功")
  }

  def end(): Unit = {
    import spark.implicits._
    val data = (TagsDateProcess.preMonthPartitions ++ TagsDateProcess.curMonthPartitions).map { case (k, v) =>
      (k._1, k._2, v)
    }.toList
    data.toDF("table_name", "detail_partition", "cur_partition").createOrReplaceTempView("t_month_update")

    sql(
      spark,
      s"""
         |INSERT OVERWRITE TABLE $monthUpdateTable PARTITION (day = '${p.day}')
         |SELECT table_name, detail_partition, cur_partition
         |FROM t_month_update
         |""".stripMargin)

    println(s"标签月更新表更新成功 总数:${data.size}, 更新数目:${TagsDateProcess.curMonthPartitions.size}")
  }

  def setCheckPoint() {
    val checkpointPath =
      s"${PropUtils.getProperty(s"mobdi.hdfs.tmp$pre")}/${p.day}"
    println("checkpoint_path:" + checkpointPath)
    spark.sparkContext.setCheckpointDir(checkpointPath)

    ShutdownHookUtils.addShutdownHook(() => {
      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
      val outPutPath = new Path(checkpointPath)
      if (fs.exists(outPutPath)) {
        fs.delete(outPutPath, true)
      }
    })
  }

}

