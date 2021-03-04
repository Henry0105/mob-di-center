package org.apache.spark

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SerializableConfiguration

/**
  * @author juntao zhang
  */
object HadoopUtils {
  def broadcastHadoopConfiguration(spark: SparkSession): Broadcast[SerializableConfiguration] = {
    spark.sparkContext.broadcast(
      new SerializableConfiguration(spark.sparkContext.hadoopConfiguration)
    )
  }
}
