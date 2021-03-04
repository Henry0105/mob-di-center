package com.youzu.mob.sdkPlus

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

object MobpushSDKTop5Appver {
  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val spark = SparkSession
      .builder()
      .appName("MobpushSDK")
      .enableHiveSupport()
      .getOrCreate()

    val kafkaAddress = args(0)
    val topic = args(1)

    val rowsRDD = spark.sql(
      s"""
         |select appkey,appver,day
         |from
         |(select appkey,appver,day,serdatetime,row_number() over (partition by appkey order by serdatetime desc) as rank
         |from
         |(select appkey,appver,day,max(serdatetime) serdatetime
         |from rp_mobdi_app.mobpush_unstall_analysis_tmp
         |where appver is not null and appver!=''
         |and appkey is not null and appkey!=''
         |and serdatetime is not null and serdatetime!=''
         |group by appkey,appver,day) t) s
         |where s.rank<=5
      """.stripMargin
    ).rdd

    val kafkaProducer = {
      val kafkaProducerConfig = {
        val props = new Properties();
        props.put("bootstrap.servers", kafkaAddress);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        props
      }
      new KafkaProducer[String, String](kafkaProducerConfig)
    }

    val jsonResult = new StringBuilder()

    // 数据量较少 直接collect后发送
    try{
      rowsRDD.collect().foreach(row => {
        val appkey = row.get(row.fieldIndex("appkey"))
        val appver = row.get(row.fieldIndex("appver"))
        val day = row.get(row.fieldIndex("day"))
        jsonResult.append("{\"appkey\":" + "\"").append(appkey).append("\"")
                  .append(",\"appver\":" + "\"").append(appver).append("\"")
                  .append(",\"day\":" + "\"").append(day).append("\"" + "}")
        kafkaProducer.send(new ProducerRecord[String, String](topic, jsonResult.toString()))
      })
      logger.info("发送数据成功，共" + rowsRDD.count() + "条数据。")
    } catch {
      case e: Exception =>
        logger.error("发送数据失败：")
        e.printStackTrace()
    } finally {
      kafkaProducer.close()
    }
  }
}
