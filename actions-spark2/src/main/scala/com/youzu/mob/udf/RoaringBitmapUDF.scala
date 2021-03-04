package com.youzu.mob.udf

import java.io.{ByteArrayOutputStream, DataOutputStream}

import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.roaringbitmap.RoaringBitmap
import java.io.{ByteArrayOutputStream, DataOutputStream, IOException}
import java.nio.ByteBuffer
import java.text.SimpleDateFormat

object RoaringBitmapUDF {

  def to_rbit(day: String): RoaringBitmap = {

    val rb = new RoaringBitmap
    val t = DateUtils.parseDate(day, "yyyyMMdd").getTime
    rb.add((t / (1000 * 3600 * 24)).toInt)
    rb.runOptimize
    rb
  }

  def to_buff(day: String): Array[Byte] = {
    val rb = new RoaringBitmap
    val t = DateUtils.parseDate(day, "yyyyMMdd").getTime
    rb.add((t / (1000 * 3600 * 24)).toInt)
    rb.runOptimize


    val b = new ByteArrayOutputStream(rb.serializedSizeInBytes)
    val d = new DataOutputStream(b)
    rb.serialize(d)
    d.close()
    b.close()
    b.toByteArray
  }

  def buff_to_rbit(buff: Array[Byte]): RoaringBitmap = {
    val rb = new RoaringBitmap
    rb.deserialize(ByteBuffer.wrap(buff))
    rb.runOptimize
    rb
  }

  def rbit_to_buff(rbit: RoaringBitmap): Array[Byte] = {
    rbit.runOptimize
    val b = new ByteArrayOutputStream(rbit.serializedSizeInBytes)
    val d = new DataOutputStream(b)
    rbit.serialize(d)
    d.close()
    b.close()
    b.toByteArray
  }

  def buff_to_value(buff: Array[Byte]): Array[String] = {
    val rb = new RoaringBitmap
    rb.deserialize(ByteBuffer.wrap(buff))
    rb.runOptimize
    val re = rb.toArray

    val fm = new SimpleDateFormat("yyyyMMdd")
    val result = for (e <- re) yield fm.format((e+1).toLong*24*3600*1000)

    result
  }


  def buff_or(a: Array[Byte],b: Array[Byte]): Array[Byte] = {
    val ra = new RoaringBitmap
    ra.deserialize(ByteBuffer.wrap(a))
    ra.runOptimize

    val rb = new RoaringBitmap
    rb.deserialize(ByteBuffer.wrap(b))


    //val rc = RoaringBitmap.or(ra,rb)
    //rc.runOptimize
    ra.or(rb)
    val e = new ByteArrayOutputStream(ra.serializedSizeInBytes)
    val f = new DataOutputStream(e)
    ra.serialize(f)
    e.close()
    f.close()
    e.toByteArray
  }


  def main(args: Array[String]): Unit = {

  }

}
