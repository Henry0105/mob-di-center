package com.youzu.mob.share

import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.sql.SparkSession
import com.youzu.mob.utils.Constants._

/**
 * @author zhoup
 *
 **/
object ShareLabel {

  def main(args: Array[String]): Unit = {

    val day = args(0)
    val p30day = args(1)
    val spark = SparkSession
      .builder()
      .appName("share label")
      .enableHiveSupport()
      .getOrCreate()

    spark.udf.register("word_cut", (str: String) =>
      new JiebaSegmenter().sentenceProcess(str).toString.split(",").distinct)


    spark.sql("select deviceid,get_json_object(content,'$.text') as text " +
      s"from $DWD_LOG_SHARE_NEW_DI where day between '$p30day' and '$day' and coalesce(trim(content),'') !=''")
      .createOrReplaceTempView("tmp_log_share_new")

    spark.sql(
      s"""
         |select deviceid,word_cut(text) as words
         |from
         |tmp_log_share_new
         |where trim(text) <>'' and text is not null
       """.stripMargin)
      .createOrReplaceTempView("tmp_words")

    spark.sql(
      s"""
         |select deviceid,share_level_mapping.level1_code,share_level_mapping.level2_code
         |from
         |(
         |  select deviceid,trim(t.word) as word
         |  from tmp_words
         |  lateral view explode(words) t as word
         |  where t.word is not null and length(trim(t.word))>0
         |)a
         |inner join
         |(select * from $DIM_SHARE_LEVEL_MAPPING where version='1000') share_level_mapping
         |on a.word = share_level_mapping.level2
       """.stripMargin)
      .createOrReplaceTempView("tmp_level_mapping")

    spark.sql(
      s"""
         |insert overwrite table $ADS_SHARE_LABEL_MONTHLY partition(day='$day')
         |select deviceid,level1_code,
         |    concat(concat_ws(',',collect_list(level2_code)),'=',concat_ws(',',collect_list(level2_cnt))) as level2_code_cnt,
         |    sum(level2_cnt) as level1_cnt
         |from
         |(
         |  select deviceid,level1_code,level2_code,count(*) as level2_cnt
         |  from tmp_level_mapping
         |  group by deviceid,level1_code,level2_code
         |)t
         |group by deviceid,level1_code
       """.stripMargin)


  }
}
