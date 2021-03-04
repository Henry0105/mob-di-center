package com.youzu.mob.newscore

import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.{Vectors => MlVectors}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object GenderScore {
  def main(args: Array[String]): Unit = {
    println(args.mkString(","))

    val logisticModelPath = args(0)
    val pre_sql = args(1)
    val length = args(2).toInt
    val out_put_table = args(3)
    val day = args(4)
    val model_app2vec = args(5)

    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    val lrModel = LogisticRegressionModel.load(logisticModelPath)

    val pre_data = spark.sql(pre_sql)

    val rdd_gender = pre_data.map(r => {
      val indexArray = r.getAs[ArrayBuffer[Int]]("index").toArray
      val valueArray = r.getAs[ArrayBuffer[Double]]("cnt").toArray
      val zipArray = indexArray.zip(valueArray).sortBy(r => r._1).unzip
      (
        r.getAs[String]("device"),
        MlVectors.sparse(length, zipArray._1, zipArray._2)
      )
    }
    ).toDF("device", "feature")

    val df_app2vec = spark.sql(
      s"""
         |select device,d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24,d25,d26,
         |       d27,d28,d29,d30,d31,d32,d33,d34,d35,d36,d37,d38,d39,d40,d41,d42,d43,d44,d45,d46,d47,d48,d49,d50,d51,d52,
         |       d53,d54,d55,d56,d57,d58,d59,d60,d61,d62,d63,d64,d65,d66,d67,d68,d69,d70,d71,d72,d73,d74,d75,d76,d77,d78,
         |       d79,d80,d81,d82,d83,d84,d85,d86,d87,d88,d89,d90,d91,d92,d93,d94,d95,d96,d97,d98,d99,d100,d101,d102,d103,
         |       d104,d105,d106,d107,d108,d109,d110,d111,d112,d113,d114,d115,d116,d117,d118,d119,d120,d121,d122,d123,d124,
         |       d125,d126,d127,d128,d129,d130,d131,d132,d133,d134,d135,d136,d137,d138,d139,d140,d141,d142,d143,d144,d145,
         |       d146,d147,d148,d149,d150,d151,d152,d153,d154,d155,d156,d157,d158,d159,d160,d161,d162,d163,d164,d165,d166,
         |       d167,d168,d169,d170,d171,d172,d173,d174,d175,d176,d177,d178,d179,d180,d181,d182,d183,d184,d185,d186,d187,
         |       d188,d189,d190,d191,d192,d193,d194,d195,d196,d197,d198,d199,d200,d201,d202,d203,d204,d205,d206,d207,d208,
         |       d209,d210,d211,d212,d213,d214,d215,d216,d217,d218,d219,d220,d221,d222,d223,d224,d225,d226,d227,d228,d229,
         |       d230,d231,d232,d233,d234,d235,d236,d237,d238,d239,d240,d241,d242,d243,d244,d245,d246,d247,d248,d249,d250,
         |       d251,d252,d253,d254,d255,d256,d257,d258,d259,d260,d261,d262,d263,d264,d265,d266,d267,d268,d269,d270,d271,
         |       d272,d273,d274,d275,d276,d277,d278,d279,d280,d281,d282,d283,d284,d285,d286,d287,d288,d289,d290,d291,d292,
         |       d293,d294,d295,d296,d297,d298,d299,d300
         |from $model_app2vec
         |where day='$day'
      """.stripMargin)

    val rdd_gender_join = rdd_gender.join(df_app2vec, Seq("device"), "left").na.fill(0.0)

    val rdd_gender_final = new VectorAssembler()
      .setInputCols(rdd_gender_join.columns.slice(1, rdd_gender_join.columns.length))
      .setOutputCol("features")
      .transform(rdd_gender_join)
      .select("device", "features")

    val predictions_logistic = lrModel.transform(rdd_gender_final)

    predictions_logistic.select("device", "prediction", "probability")
      .map(r => (r.getString(0), r.getDouble(1),
        r.getAs[org.apache.spark.ml.linalg.Vector](2).apply(r.getDouble(1).toInt)))
      .toDF("device", "prediction", "probability")
      .repartition(10)
      .createOrReplaceTempView("lr_scoring")

    spark.sql(
      s"""
         |insert overwrite table $out_put_table partition (day = $day, kind = 'gender')
         |select device, prediction, probability
         |from lr_scoring
         """.stripMargin)
  }
}
