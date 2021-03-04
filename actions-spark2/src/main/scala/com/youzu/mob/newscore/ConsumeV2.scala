package com.youzu.mob.newscore

import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.SparkSession


case class ConsumeV2(@transient spark: SparkSession) {
  def score(args: Array[String]): Unit = {
    val day = args(0)
    val seed = args(1)
    val mapping = args(2)
    val output_table = args(3)
    val model_path = args(4)
    val model_path2 = args(5)
    val train_flag = args(6)

    spark.sql(s"$seed").createOrReplaceTempView("seed")
    //spark.sql("""
    // add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    // """ )
    //spark.sql("create temporary function array_intersect as 'com.youzu.mob.java.udf.ArrayIntersectUDF'")

    spark.sql(
      s"""
         |select device,type,size(array_intersect(split(applist, ','),pkgs)) as f_cnt from seed a
         |cross join
         |(
         |select collect_set(pkg) as pkgs,type from $mapping where version='1000' group by type
         |)b
         |""".stripMargin).repartition(1000).groupBy("device").pivot("type").sum("f_cnt").createOrReplaceTempView("cluster")


    //spark.sql("cache table cluster")
    var comsume_data = spark.sql("select device,bank,beauty,beauty_borrow,borrow,card,fans,haitao,highend_shop,legend_game,luxury_car,makeup,money_game,photo,shopping from  cluster")
    //将特征变成稠密向量

    var assemblerZs = {
      new VectorAssembler()
        .setInputCols(comsume_data.columns.slice(1, comsume_data.columns.length - 1))
        .setOutputCol("features")
        .transform(comsume_data)
        .select("device", "features")
    }

    var zscaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true).setWithMean(true)

    var zscalerModel = zscaler.fit(assemblerZs)
    var assemblerZs_zscale = zscalerModel.transform(assemblerZs)


    var model_zscale = KMeansModel.load(s"$model_path")

    var predictions_zscale = model_zscale.transform(assemblerZs_zscale)
    predictions_zscale.createOrReplaceTempView("result_zscale")
    spark.sql("cache table result_zscale")
    predictions_zscale.groupBy("prediction").count().show

    // 轮廓系数（zscore要好于极差标准化，使用zscore的结果）
    var evaluator = new ClusteringEvaluator()

    var silhouette1 = evaluator.evaluate(predictions_zscale)
    // Double = 0.44623700933532967（只放在装，6类）

    println("evaluation:" + silhouette1)

    // 将比较多的pre=4拿出来再聚一次
    val split_cluster = spark.sql(
      s"""
         |select prediction from result_zscale group by prediction order by count(1) desc limit 1
         |""".stripMargin).rdd.map(p => p.getInt(0)).collect().last

    val seed2 = spark.sql(
      s"""
         |select a.device,bank,beauty,beauty_borrow,borrow,card,fans,haitao,highend_shop,legend_game,luxury_car,makeup,money_game,photo,shopping from cluster a
         |left join result_zscale b
         |on a.device=b.device
         |where prediction=$split_cluster and
         |!(luxury_car=0 and beauty=0 and haitao=0 and beauty_borrow=0 and shopping=0 and photo=0 and makeup=0
         |and highend_shop=0 and borrow=0 and card=0 and bank=0 and fans=0 and legend_game=0 and money_game=0
         |)
         |""".stripMargin)

    var assemblerZs2 = {
      new VectorAssembler()
        .setInputCols(seed2.columns.slice(1, seed2.columns.length ))
        .setOutputCol("features")
        .transform(seed2)
        .select("device", "features")
    }

    var zscaler2 = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true).setWithMean(true)

    var zscalerModel2 = zscaler2.fit(assemblerZs2)
    var assemblerZs_zscale2 = zscalerModel2.transform(assemblerZs2)


    var model_zscale2 = KMeansModel.load(s"$model_path2")

    var predictions_zscale2 = model_zscale2.transform(assemblerZs_zscale2)

    predictions_zscale2.createOrReplaceTempView("result_zscale2")
    //spark.sql("cache table result_zscale2")

    predictions_zscale2.groupBy("prediction").count().show

    var evaluator2 = new ClusteringEvaluator()

    var silhouette2 = evaluator2.evaluate(predictions_zscale2)
    println("evaluation2:" + silhouette2)

    //输出两次聚类的结果，至于逻辑自洽，放到外面做
    spark.sql(
      s"""
         |insert overwrite table $output_table partition (day='$day')
         |
         |select a.device,bank,beauty,beauty_borrow,borrow,card,fans,haitao,highend_shop,legend_game,luxury_car,makeup,money_game,photo,shopping,b.prediction as prediction_only_install,c.prediction as cluster4_prediction_nooffline
         |,null as cluster
         |from cluster a
         |left join result_zscale b
         |on a.device=b.device
         |left join result_zscale2 c
         |on a.device=c.device
         |""".stripMargin).createOrReplaceTempView("unioned")


  }

  def train(args: Array[String]): Unit = {
    val day = args(0)
    val seed = args(1)
    val mapping = args(2)
    val output_table = args(3)
    val model_path = args(4)
    val model_path2 = args(5)
    val train_flag = args(6)

    spark.sql(s"$seed").createOrReplaceTempView("seed")
    //spark.sql("""
    // add jar hdfs://ShareSdkHadoop/dmgroup/dba/commmon/udf/udf-manager-0.0.7-SNAPSHOT-jar-with-dependencies.jar;
    // """ )
    //spark.sql("create temporary function array_intersect as 'com.youzu.mob.java.udf.ArrayIntersectUDF'")

    spark.sql(
      s"""
         |select device,type,size(array_intersect(split(applist, ','),pkgs)) as f_cnt from seed a
         |cross join
         |(
         |select collect_set(pkg) as pkgs,type from $mapping where version='1000' group by type
         |)b
         |""".stripMargin).repartition(1000).groupBy("device").pivot("type").sum("f_cnt").createOrReplaceTempView("cluster")

    //spark.sql("cache table cluster")
    var comsume_data = spark.sql("select device,bank,beauty,beauty_borrow,borrow,card,fans,haitao,highend_shop,legend_game,luxury_car,makeup,money_game,photo,shopping from  cluster")
    //将特征变成稠密向量

    var assemblerZs = {
      new VectorAssembler()
        .setInputCols(comsume_data.columns.slice(1, comsume_data.columns.length - 1))
        .setOutputCol("features")
        .transform(comsume_data)
        .select("device", "features")
    }

    var zscaler = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true).setWithMean(true)

    var zscalerModel = zscaler.fit(assemblerZs)
    var assemblerZs_zscale = zscalerModel.transform(assemblerZs)


    var kmeans = new KMeans().setK(6).setFeaturesCol("scaledFeatures").setPredictionCol("prediction").setSeed(1L)
    var model_zscale = kmeans.fit(assemblerZs_zscale)
    model_zscale.write.overwrite().save(s"$model_path")

    var predictions_zscale = model_zscale.transform(assemblerZs_zscale)
    predictions_zscale.createOrReplaceTempView("result_zscale")
    predictions_zscale.groupBy("prediction").count().show

    // 轮廓系数（zscore要好于极差标准化，使用zscore的结果）
    var evaluator = new ClusteringEvaluator()

    var silhouette1 = evaluator.evaluate(predictions_zscale)
    // Double = 0.44623700933532967（只放在装，6类）

    println("evaluation:" + silhouette1)

    // 将比较多的pre=4拿出来再聚一次
    val split_cluster = spark.sql(
      s"""
         |select prediction from result_zscale group by prediction order by count(1) desc limit 1
         |""".stripMargin).rdd.map(p => p.getInt(0)).collect().last

    val seed2 = spark.sql(
      s"""
         |select a.device,bank,beauty,beauty_borrow,borrow,card,fans,haitao,highend_shop,legend_game,luxury_car,makeup,money_game,photo,shopping from cluster a
         |left join result_zscale b
         |on a.device=b.device
         |where prediction=$split_cluster and
         |!(luxury_car=0 and beauty=0 and haitao=0 and beauty_borrow=0 and shopping=0 and photo=0 and makeup=0
         |and highend_shop=0 and borrow=0 and card=0 and bank=0 and fans=0 and legend_game=0 and money_game=0
         |)
         |""".stripMargin)

    var assemblerZs2 = {
      new VectorAssembler()
        .setInputCols(seed2.columns.slice(1, seed2.columns.length - 1))
        .setOutputCol("features")
        .transform(seed2)
        .select("device", "features")
    }

    var zscaler2 = new StandardScaler().setInputCol("features").setOutputCol("scaledFeatures").setWithStd(true).setWithMean(true)

    var zscalerModel2 = zscaler2.fit(assemblerZs2)
    var assemblerZs_zscale2 = zscalerModel2.transform(assemblerZs2)


    var kmeans2 = new KMeans().setK(9).setFeaturesCol("scaledFeatures").setPredictionCol("prediction").setSeed(1L)
    var model_zscale2 = kmeans2.fit(assemblerZs_zscale)
    model_zscale2.write.overwrite().save(s"$model_path2")

    var predictions_zscale2 = model_zscale2.transform(assemblerZs_zscale2)

    predictions_zscale2.createOrReplaceTempView("result_zscale2")

    predictions_zscale2.groupBy("prediction").count().show

    var evaluator2 = new ClusteringEvaluator()

    var silhouette2 = evaluator2.evaluate(predictions_zscale2)
    println("evaluation2:" + silhouette2)

  }
}
object ConsumeV2 {
  def main(args: Array[String]): Unit = {
    var day = args(0)
    var seed = args(1)


    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$") + s"_$day")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", 1000)


    val train_flag = args(6)

    if(train_flag.equals("1")) {
      new ConsumeV2(spark).train(args)
    } else {
      new ConsumeV2(spark).score(args)
    }

  }
}
