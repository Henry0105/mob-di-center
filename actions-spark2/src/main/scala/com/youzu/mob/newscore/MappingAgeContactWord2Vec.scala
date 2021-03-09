package com.youzu.mob.newscore

import com.youzu.mob.tools.SparkEnv
import com.youzu.mob.utils.Constants.DM_MOBDI_TMP
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.ansj.domain.Term
import org.ansj.splitWord.analysis.{BaseAnalysis, DicAnalysis, NlpAnalysis, ToAnalysis}
import org.ansj.library.DicLibrary

import java.util.Arrays
import scala.io.Source
import java.io._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.mllib.clustering.{DistributedLDAModel, LDA}

import java.io.File
import org.ansj.recognition.impl.StopRecognition
import org.nlpcn.commons.lang.tire.domain.Forest
import org.nlpcn.commons.lang.tire.domain.Value
import org.nlpcn.commons.lang.tire.library.Library
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.JavaConverters._
import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, HashingTF, IDF, QuantileDiscretizer, Tokenizer, Word2Vec, Word2VecModel}
import org.apache.spark.ml.linalg.{SparseVector, Vector}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.DenseVector


class MappingAgeContactWord2Vec(@transient spark: SparkSession) {
  def score(args: Array[String]): Unit = {
    val day = args(0)
    val phone_contact_version = args(1)
    val model_path = args(2)
    val output_table = args(3)
    import spark.implicits._

    val UDFRetrieve1 = udf { (v: SparseVector) =>
      v.indices
    }
    val UDFRetrieve2 = udf { (v: SparseVector) =>
      v.values
    }

    var DataOrigin = spark.sql(
      s"""
         |    select phone,concat_ws(',',words_list) wordslist
         |    from $DM_MOBDI_TMP.phone_contacts_index_word_split_prepare
         |    where day='$phone_contact_version'
  """.stripMargin)

    val testData = DataOrigin.withColumn("wordslist", concat_ws(" ", col("wordslist")))

    val filter = new StopRecognition()
    filter.insertStopNatures("w")
    val stopWords = List(" ", "", "／", "·", "；", "＂", "．", "％", "：", "］", "［", "－", "\"\"")
    stopWords.foreach((a: String) => filter.insertStopWords(a))

    val UDFwords = udf { (x: String) =>
      BaseAnalysis.parse(x).recognition(filter).toStringWithOutNature("/").split("/")
    }

    val temp3 = testData.withColumn("splitwords", UDFwords(col("wordslist")))

    val model_2 = Word2VecModel.load(model_path)

    val result_2 = model_2.transform(temp3)


    val toArr: Any => Array[Double] = _.asInstanceOf[DenseVector].toArray
    val toArrUdf = udf(toArr)
    val result__2 = result_2.withColumn("features_arr", toArrUdf(col("result")))

    result__2.createOrReplaceTempView("tmpoutput")
    spark.sql(
      s"""insert overwrite table $output_table partition (day='$day')
         |select phone,features_arr w2v_100 from tmpoutput""".stripMargin)

  }
}

object MappingAgeContactWord2Vec {

  def main(args: Array[String]): Unit = {
    val day = args(0)
    val spark = SparkEnv.initial(s"mapping_age_contact_word2vec_${day}")

    new MappingAgeContactWord2Vec(spark).score(args)
  }
}