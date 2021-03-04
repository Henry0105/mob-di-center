package com.youzu.mob.industrytags

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object GenerateEstimatePartitionTable {
  Logger.getLogger("org").setLevel(Level.ERROR)

  val baseProfileTable: String = "rp_sdk_dmp.rp_device_profile_full"
  val outputTable: String = "dm_sdk_mapping.offline_esti_config"

  def getOutputTableDF(
    spark: SparkSession, inputTable: String, featureType: String,
    baseProfileColumnsSeq: Seq[String], weightField: String, conditionRules: Map[String, String]
  ): (String, DataFrame) = {
    val featureDF = spark.sql(s"SELECT * FROM $inputTable LIMIT 1")

    val featureColumnsSeq = featureDF.columns.toSeq.diff(Seq(weightField))
    val baseJoinFieldsSeq = baseProfileColumnsSeq.intersect(featureColumnsSeq)
    val conditionFieldsOrigSeq = featureDF.columns.filter(_.startsWith("__")).toSeq
    val conditionFieldsSeq = featureDF.columns.filter(
      _.startsWith("__")).map(_.substring(2)).toSeq
    val extendFieldsSeq = featureColumnsSeq.diff(
      baseProfileColumnsSeq).diff(conditionFieldsOrigSeq)

    val groupId = Utils.seq2Str(baseJoinFieldsSeq)
    val conditionFields = Utils.seq2Str(conditionFieldsSeq)
    val groupExt = Utils.seq2Str(extendFieldsSeq)

    var conditionMap: Map[String, String] = Map()
    for (condition <- conditionFieldsSeq) {
      val key = inputTable + "." + condition
      if (conditionRules.contains(key)) {
        conditionMap += (condition -> conditionRules.getOrElse(key, ""))
      } else {
        throw new RuntimeException("CAN NOT FIND CASE WHEN CONDITION: " + key + " IN SUBMIT SHELL FILE!")
      }
    }

    val exe_sql =
      s"""
          SELECT
          '$groupId' as group_id,
          '$conditionFields' as group_condition,
          map(${Utils.seq2MapStr(baseJoinFieldsSeq)}) as base_field,
          map(${Utils.seq2MapStr(conditionFieldsOrigSeq)}) as condition_field,
          map(${Utils.seq2MapStr(extendFieldsSeq)}) as extend_field,
          $weightField as weight_field,
          '${Utils.map2Base64Str(conditionMap)}' as condition_rules
          FROM $inputTable
       """.stripMargin
    println(exe_sql)
    (groupExt, spark.sql(exe_sql))
  }

  def main(args: Array[String]): Unit = {

    var featureType = ""
    var featureTables: Array[String] = null
    var weightField = ""
    var conditionRules: Map[String, String] = Map()

    if (args.length != 4) {
      println("parameter error")
      sys.exit(-1)
    }

    for (arg <- args) {
      if (arg.toLowerCase.startsWith("feature_type=")) featureType = arg.split("=")(1).trim
      else if (arg.toLowerCase.startsWith("feature_tables=")) featureTables = arg.split("=")(1).
        trim.split(";").map(table => table.trim)
      else if (arg.toLowerCase.startsWith("weight_field=")) weightField = arg.split("=")(1).trim
      else if (arg.toCharArray.startsWith("condition_rule=")) {
        if (arg.split("=", 2)(1).length > 0) {
          arg.split("=", 2)(1).split(";").filter(_.contains(":")).foreach(
            rule => conditionRules += (rule.split(":")(0).trim -> rule.split(":")(1).trim)
          )
        }
      }
    }

    val conf = new SparkConf().setAppName("Generate Estimated Partition Table: " + outputTable)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val baseProfileColumnsSeq = spark.sql(
      s"SELECT * FROM $baseProfileTable LIMIT 1"
    ).columns.toSeq

    var resMap: Map[String, DataFrame] = Map()
    for (featureTable <- featureTables) {
      val (groupExt, resDF) = getOutputTableDF(
        spark, featureTable, featureType, baseProfileColumnsSeq, weightField, conditionRules)
      if (resMap.contains(groupExt)) {
        val resMapValue = resMap.get(groupExt)
        resMap += (groupExt -> resDF.union(resMapValue.get))
      } else {
        resMap += (groupExt -> resDF)
      }
    }

    for (entry <- resMap) {
      val groupExt = entry._1
      val resDF = entry._2
      resDF.createOrReplaceTempView("RES_TABLE")
      spark.sql(
        s"""
           |INSERT OVERWRITE TABLE $outputTable PARTITION (feature_type='$featureType', group_ext='$groupExt')
           |SELECT * FROM RES_TABLE
         """.stripMargin)
    }

    println("FEATURE TABLE IS OK")

  }
}
