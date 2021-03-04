package com.youzu.mob.industrytags

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.log4j.{Level, Logger}

object CalculateIndustryTagsForGroup {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val baseProfileTable: String = "rp_sdk_dmp.rp_device_profile_full"
  val featureTable: String = "dm_sdk_mapping.offline_esti_config"
  val weightField: String = "weight_field"

  def printUsage(): Unit = {
    println("Usage:")
    println("\t parameter: device_table=xxx device_field=xxx feature_type=xxx output_table=xxx")
  }

  def getOneFeatureTypeDF(spark: SparkSession, featureType: String,
    groupId: String, groupExt: String, groupCondition: String, conditionRulesMap: Map[String, String]
  ): DataFrame = {

    val baseSql =
      s"""
         |SELECT
         |device,
         |$groupId${Utils.map2CaseWhenStr(conditionRulesMap)}
         |FROM BASE_PROFILE
       """.stripMargin

    val baseDF = spark.sql(baseSql)

    val groupConditionStr = if (groupCondition != null && groupCondition.nonEmpty) {
      Utils.getMapKeyValueStr("condition_field", groupCondition) + ","
    } else {
      ""
    }

    val featureSql =
      s"""
         |SELECT
         |${Utils.getMapKeyValueStr("base_field", groupId)}, $groupConditionStr
         |${Utils.getMapKeyValueStr("extend_field", groupExt)},
         |$weightField
         |FROM $featureTable
         |WHERE
         |feature_type='$featureType'
         |AND group_ext='$groupExt'
         |AND group_id='$groupId'
       """.stripMargin

    val featureDF = spark.sql(featureSql)

    val joinFields = groupId + {
      if (groupCondition != null && groupCondition.nonEmpty) {
        "," + groupCondition
      } else {
        ""
      }
    }

    baseDF.join(
      featureDF,
      joinFields.split(",").map(str => str.trim).toSeq,
      "leftouter"
    ).createOrReplaceTempView("SELECTED_EXTEND_PROFILE")

    val totalCnt = spark.sql(s"SELECT device FROM SELECTED_EXTEND_PROFILE GROUP BY device").count()
    if (totalCnt == 0) {
      throw new RuntimeException("Total device count is zero!")
    }

    val calc_sql =
      s"""
         |SELECT
         |map(${Utils.seq2MapStr(groupExt.split(",").map(str => str.trim).toSeq)}) as extend_field,
         |round(sum(t2.percent), 6) as sum_percent
         |FROM
         |  (
         |  SELECT
         |  $groupId,
         |  $groupExt,
         |  ${
        if (groupCondition != null && groupCondition.nonEmpty) {
          groupCondition + ","
        } else {
          ""
        }
      }
         |  round(t1.cnt * t1.$weightField / $totalCnt, 6) AS percent
         |  FROM
         |  (
         |    SELECT
         |    $groupId,
         |    $groupExt,
         |    ${
        if (groupCondition != null && groupCondition.nonEmpty) {
          groupCondition + ","
        } else {
          ""
        }
      }
         |    $weightField,
         |    count(1) AS cnt
         |    FROM SELECTED_EXTEND_PROFILE
         |    GROUP BY $groupId,$groupExt,${
        if (groupCondition != null && groupCondition.nonEmpty) {
          groupCondition + ","
        } else {
          ""
        }
      }$weightField
         |  ) t1
         |) t2
         |GROUP BY $groupExt
         |HAVING sum_percent IS NOT NULL
       """.stripMargin
    println(calc_sql)
    spark.sql(calc_sql)
  }


  def main(args: Array[String]): Unit = {
    var outputTable: String = ""
    var deviceTable: String = ""
    var featureType: String = ""
    var deviceField: String = "device"

    for (arg <- args) {
      if (arg.toLowerCase.startsWith("output_table=")) outputTable = arg.split("=")(1).trim
      else if (arg.toLowerCase.startsWith("device_table=")) deviceTable = arg.split("=")(1).trim
      else if (arg.toLowerCase.startsWith("feature_type=")) featureType = arg.split("=")(1).trim
      else if (arg.toLowerCase.startsWith("device_field=")) deviceField = arg.split("=")(1).trim
    }

    if (outputTable == "" || deviceTable == "" || featureType == "" || deviceField == "" || weightField == "") {
      println("parameter error")
      printUsage()
      sys.exit(-1)
    } else {
      println(
        s"""
           |input args:
           |    device_table=$deviceTable
           |    device_field=$deviceField
           |    feature_type=$featureType
           |    output_table=$outputTable
           |
           |base profile table:
           |    $baseProfileTable
           |feature table:
           |    $featureTable
           |
         """.stripMargin
      )
    }

    val conf = new SparkConf().setAppName("INDUSTRY TAG - " + featureType + " - " + deviceTable + " - " + outputTable)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    /**
     * SELECT group_id, group_condition, group_ext, condition_rules
     * FROM dm_sdk_mapping.offline_esti_config2
     * WHERE feature_type = '9'
     * AND group_id <> '' AND group_ext <> ''
     * GROUP BY group_id, group_condition, group_ext, condition_rules
     */
    val GroupSql =
      s"""
         |SELECT group_id, group_condition, group_ext, condition_rules
         |FROM $featureTable
         |WHERE feature_type = '$featureType'
         |AND group_id <> '' AND group_ext <> ''
         |GROUP BY group_id, group_condition, group_ext, condition_rules
       """.stripMargin
    val GroupDF = spark.sql(GroupSql)

    val rows = GroupDF.collect()

    var mergedBaseFieldsSeq = Seq("")
    var mergedConditionFieldsSeq = Seq("")
    for (row <- rows) {
      val groupIdSeq = row.getAs[String]("group_id").split(",").map(str => str.trim).toSeq
      mergedBaseFieldsSeq = mergedBaseFieldsSeq.union(groupIdSeq)

      val conditionFieldsSeq = row.getAs[String]("group_condition").split(",").map(str => str.trim).toSeq
      mergedConditionFieldsSeq = mergedConditionFieldsSeq.union(conditionFieldsSeq)
    }
    val finalMergedFieldsSeq = mergedBaseFieldsSeq.union(mergedConditionFieldsSeq).distinct.diff(Seq(""))

    val mergeBaseProfileSql =
      s"""
         |SELECT
         |device,
         |${Utils.seq2Str(finalMergedFieldsSeq)}
         |FROM $baseProfileTable
       """.stripMargin
    val mergedBaseProfileDF = spark.sql(mergeBaseProfileSql)


    val deviceDF = spark.sql(s"SELECT $deviceField as device FROM $deviceTable GROUP BY $deviceField")

    mergedBaseProfileDF.join(
      deviceDF,
      Seq("device"),
      "leftsemi"
    ).createOrReplaceTempView("SELECTED_MERGED_BASE_PROFILE")


    spark.sql(s"CACHE TABLE BASE_PROFILE AS SELECT * FROM SELECTED_MERGED_BASE_PROFILE")



    var resDF = spark.emptyDataFrame
    for (row <- rows) {
      val groupId = row.getAs[String]("group_id")
      val groupExt = row.getAs[String]("group_ext")
      val groupCondition = row.getAs[String]("group_condition")
      val conditionRulesMap = Utils.base64Str2Map(row.getAs[String]("condition_rules"))
      if (groupId != null && groupExt != null) {
        val tmpDF = getOneFeatureTypeDF(spark, featureType, groupId, groupExt, groupCondition, conditionRulesMap)
        println(tmpDF.count())
        if (resDF == spark.emptyDataFrame) {
          resDF = tmpDF
        } else {
          resDF = resDF.union(tmpDF)
        }
      }
    }

    resDF.
      repartition(1).
      write.mode(SaveMode.Overwrite).
      saveAsTable(outputTable)

    println("FINISHED SUCCESSFULLY")
    spark.stop()
  }

}
