package com.youzu.mob.ppx

import com.youzu.mob.utils.Constants._
import org.apache.spark.sql.SparkSession

object PPXScore {

  def main(args: Array[String]): Unit = {

    val day = args(0)

    val spark = SparkSession
      .builder()
      .appName("ppx_score")
      .enableHiveSupport()
      .getOrCreate()


    spark.udf.register("app_distinct", (applist: String) => {
      val pkg = applist.split(",")
      pkg.flatMap(_.split("\\.")).distinct.mkString(",")
    })

    spark.udf.register("ppx_score", (t1: Seq[String]) => {
      if (t1 == null || t1.isEmpty || t1.size == 0) {
        "0.825234800758695,0.5"
      } else {
        var appScore = t1.filter(_.length > 0).map(x => {
          val rs = x.split(",")
          (rs(0).toDouble, rs(1).toInt)
        }).groupBy(_._2).mapValues(_.map(_._1).sum)

        if (!appScore.keySet.contains(1)) {
          appScore += (1 -> 0)
        }
        if (!appScore.keySet.contains(2)) {
          appScore += (2 -> 0)
        }

        val result = appScore.map(map => {
          val key = map._1
          val value = map._2
          key match {
            case 1 => (key, 1 / (1 + Math.exp(value - 1.55222459843)))
            case _ => (key, 1 / (1 + Math.exp(value + 0)))
          }
        }).toSeq.sortBy(_._1)
        List(result(0)._2, result(1)._2).mkString(",")
      }
    }
    )

    spark.sql("create temporary function ppx_encode as 'com.youzu.mob.java.udf.PPXEncode'")

    val scoreDF = spark.sql(
      s"""
         |select device,ppx_score(collect_set(coef_group)) as scores
         |from
         |(
         |  select device,concat_ws(',',coef,group) as  coef_group
         |  from
         |  (
         |    select device, ppx_encode(t.app) as app_encode
         |    from $DEVICE_PROFILE_LABEL_FULL_PAR
         |    lateral view explode(split(app_distinct(applist),',')) t as app
         |    where version='${day}.1000'
         |  )profile_full
         |  left join
         |  $DIM_PPX_APP_MAPPING
         |  on profile_full.app_encode = ppx_app.app_wd
         |)t
         |group by device
         """.stripMargin)
    scoreDF.createOrReplaceTempView("tmp_score")
    spark.sql(
      s"""
         |select device,scores,score1,score2,
         |      -0.65495-15.39235*score1+33.02434*score2
         |          +70.11117*pow(score1,2)-148.08092*pow(score2,2)
         |          -138.68910*pow(score1,3)+363.12489*pow(score2,3)
         |          +145.17917*pow(score1,4)-502.21698*pow(score2,4)
         |          -80.05083*pow(score1,5)+367.58095*pow(score2,5)
         |          +18.46553*pow(score1,6)-110.80924*pow(score2,6)
         |          -2.23579*(score1*score2)
         |          -60.19228*(pow(score1,2)*pow(score2,2))
         |          +267.07384*(pow(score1,3)*pow(score2,3))
         |          -482.03755*(pow(score1,4)*pow(score2,4))
         |          +410.35892*(pow(score1,5)*pow(score2,5))
         |          -136.12235*(pow(score1,6)*pow(score2,6)) as probability,
         |      2985.90-35602.94*score1+4750.30*score2+155272.24*pow(score1,2)
         |          -11750.99*pow(score2,2)-343194.19*pow(score1,3)
         |          +21200.85*pow(score2,3)+415622.31*pow(score1,4)
         |          -22928.18*pow(score2,4)-263143.82*pow(score1,5)
         |          +13395.11*pow(score2,5)+68384.89*pow(score1,6)
         |          -2973.81*pow(score2,6)-3987.08*(score1*score2)
         |          +15850.68*(pow(score1,2)*pow(score2,2))-42061.83*(pow(score1,3)*pow(score2,3))
         |          +67087.48*(pow(score1,4)*pow(score2,4))-58252.65*(pow(score1,5)*pow(score2,5))
         |          +21114.00*(pow(score1,6)*pow(score2,6)) as score
         |from
         |(
         |  select device,scores,cast(split(scores,',')[0] as double) as score1,cast(split(scores,',')[1] as double) as score2
         |  from tmp_score
         |)t
       """.stripMargin).createOrReplaceTempView("score_result")

    spark.sql(
      s"""
         |insert overwrite table $ADS_PPX_SCORE_WEEKLY partition(day=${day})
         |select device, score1, score2,
         |  case when probability <0 then 0
         |       when probability >1 then 1
         |       else probability
         |  end as probability,
         |  case
         |  when probability < 0 then 800
         |  when probability > 1 then 300
         |  when round(score) > 800 then 800
         |  when round(score) < 300 then 300
         |  else round(score)
         |  end as score,
         |  case
         |  when probability < 0 then  'A'
         |  when probability > 1 then  'I'
         |  when round(score) >= 700 then 'A'
         |  when round(score) >= 658 and round(score) <= 699 then 'B'
         |  when round(score) >= 626 and round(score) <= 657 then 'C'
         |  when round(score) >= 599 and round(score) <= 625 then 'D'
         |  when round(score) >= 574 and round(score) <= 598 then 'E'
         |  when round(score) >= 553 and round(score) <= 573 then 'F'
         |  when round(score) >= 530 and round(score) <= 552 then 'G'
         |  when round(score) >= 496 and round(score) <= 529 then 'H'
         |  else 'I'
         |  end as risk_rank
         |from score_result
       """.stripMargin)
  }
}