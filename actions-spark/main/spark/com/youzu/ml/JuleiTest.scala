package com.youzu.ml

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkContext, SparkConf, sql}


object JuleiTest {

  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val sqlContext = new sql.hive.HiveContext(sc)
    import sqlContext.implicits._
    val sql_str = args(0)

    val trainRepartitionNum = Integer.parseInt(args(1))
    val IterNum = Integer.parseInt(args(2))

    val rp_mobeye_tfidf_pca = sqlContext.sql(sql_str).repartition(trainRepartitionNum)
    val rp_mobeye_tfidf_pca_rdd = rp_mobeye_tfidf_pca.map { e =>
      val tfidf_list = Vectors.dense(e.get(1).toString.split(',').map(_.toDouble))
      tfidf_list
    }

    val splits = rp_mobeye_tfidf_pca_rdd.randomSplit(Array(0.8, 0.2))
    val rp_mobeye_tfidf_pca_rdd_training = splits(0).cache()
    val numClusters = Integer.parseInt(args(3))
    val clusters_model = KMeans.train(rp_mobeye_tfidf_pca_rdd_training, numClusters, IterNum)
    clusters_model.save(sc, "/user/zhangpe/KMeans/KMeansModel")

  }
}
