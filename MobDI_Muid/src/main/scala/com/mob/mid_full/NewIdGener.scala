package com.mob.mid_full

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

/**
 * @author zhoup
 * 1.为所有设备编号,imei编号0-20亿,mac 20-40亿,serialno 40亿+
 * 2.编号作为边的顶点id运行graphx生成连通子图,把同一设备的imei,mac,serialno放到同一个list里
 *   即把imei,mac,serialno中任一个字段相同的设备视为同一个device
 * 3.使用imei和mac分别生成deviceid,没有serialno作为顶点的子图
 * 4.按顺序取imei,mac,serialno生成的deviceid中的第一个非空的deviceid作为设备的deviceid
 **/
object NewIdGener {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("graphx").enableHiveSupport().getOrCreate()
    spark.sql("create  temporary function  sha3 as 'com.youzu.mob.java.udf.SHA1Hashing'")
    val newDF = spark.sql(
      s"""
         |select nvl(imei,'') as imei,nvl(mac,'') as mac,nvl(serialno,'') as serialno,nvl(model,'') as model,min_dt,max_dt,
         |      case when id_imei=-1 then cast(rand() * -3000000000 as long) else id_imei end as id_imei,
         |      case when id_mac =-1 then cast(rand() * -1000000000 as long) else  id_mac+2000000000 end as id_mac,
         |      case when id_serino=-1 then cast(rand() * -2000000000 as long) else id_serino+4000000000 end as id_serino
         |     from test.device_uni_pre2
       """.stripMargin).persist(StorageLevel.MEMORY_AND_DISK_SER)

    newDF.createOrReplaceTempView("tmp_id_incr")

    import spark.implicits._
    val df = spark.sql(
       s"""
          |select imei as src ,mac as dst,id_imei as src_id,id_mac as dst_id
          |from tmp_id_incr
          |where length(imei) >0 and length(mac)>0
          |group by imei,mac,id_imei,id_mac
          |union all
          |select imei as src ,serialno as dst,id_imei as src_id,id_serino as dst_id
          |from tmp_id_incr
          |where length(imei) >0 and length(serialno)>0
          |group by imei,serialno,id_imei,id_serino
          |union all
          |select mac as src ,serialno as dst,id_mac as src_id,id_serino as dst_id
          |from tmp_id_incr
          |where length(mac) >0 and length(serialno)>0
          |group by mac,serialno,id_mac,id_serino
        """.stripMargin).repartition(2000).persist(StorageLevel.MEMORY_AND_DISK_SER)

     val infos = df.mapPartitions(rows => {
       rows.flatMap(row => {
         val array = new ArrayBuffer[(VertexId, String)]()
         val src = row.getAs[String]("src")
         val dst = row.getAs[String]("dst")
         val src_id = row.getAs[Long]("src_id")
         val dst_id = row.getAs[Long]("dst_id")
         array.append((src_id,src))
         array.append((dst_id,dst))
         array
       })
     }).rdd

     val rela: RDD[Edge[VertexId]] = df.mapPartitions(rows => {
       rows.flatMap(row => {
         val array = new ArrayBuffer[Edge[Long]]()
         val src_id = row.getAs[Long]("src_id")
         val dst_id = row.getAs[Long]("dst_id")
         array.append(Edge(src_id,dst_id))
         array
       })
     }).rdd


     val graph = Graph(infos, rela)


     graph.connectedComponents().vertices.groupBy(_._2).mapValues(values => {
       values.flatMap(value => {
         Iterator(value._1.toLong)
       })
     }).map(x => {
       (x._1.toLong, x._2.asInstanceOf[Seq[Long]])
     }).toDF("id", "id_list").createOrReplaceTempView("tmp_graphx")

    // spark.sql("create table test.graphx_id_result4 as select * from tmp_graphx")


    // spark.sql("select * from test.graphx_id_result3").persist(StorageLevel.MEMORY_AND_DISK_SER).createOrReplaceTempView("tmp_graphx")

    spark.sql(
      s"""
         |select sha3(concat(imei,mac,serialno,model)) as deviceid,id_list
         |from
         |(
         |select imei,mac,serialno,model,id_imei,min_dt,id_list,row_number() over(partition by id_imei order by min_dt) as rank
         |from
         |(
         |select imei,mac,serialno,model,id_imei,min_dt,id_list,min_dt
         |from tmp_id_incr a
         |inner join
         |(select * from tmp_graphx where id < 2000000000) v_imei
         |on a.id_imei=v_imei.id
         |)t
         |)t2
         |where rank = 1
       """.stripMargin).createOrReplaceTempView("t_id_imei")

    spark.sql(
      s"""
         |select sha3(concat(imei,mac,serialno,model)) as deviceid,id_list
         |from
         |(
         |select imei,mac,serialno,model,id_mac,min_dt,id_list,row_number() over(partition by id_mac order by min_dt) as rank
         |from
         |(
         |select imei,mac,serialno,model,id_mac,min_dt,id_list,min_dt
         |from tmp_id_incr a
         |inner join
         |(select * from tmp_graphx where id between 2000000000 and 4000000000 ) v_mac
         |on a.id_mac=v_mac.id
         |)t
         |)t2
         |where rank = 1
       """.stripMargin).createOrReplaceTempView("t_id_mac")

    spark.sql(
      s"""
         |select imei,mac,serialno,model,min_dt,max_dt,a.id_imei,a.id_mac,a.id_serino,
         |coalesce(t_id_imei.deviceid,t_id_mac.deviceid,t_id_serino.deviceid) as deviceid
         |from tmp_id_incr a
         |left join
         |(select t.id ,deviceid from t_id_imei lateral view explode(id_list) t as id) t_id_imei
         |on a.id_imei = t_id_imei.id
         |left join
         |(select t.id ,deviceid from t_id_mac lateral view explode(id_list) t as id
         | union all
         | select t1.id ,deviceid from t_id_imei lateral view explode(id_list) t1 as id
         |) t_id_mac
         |on a.id_mac = t_id_mac.id
         |left join
         |(
         |select t.id ,deviceid from t_id_mac lateral view explode(id_list) t as id
         | union all
         |select t1.id ,deviceid from t_id_imei lateral view explode(id_list) t1 as id
         |)t_id_serino
         |on a.id_serino = t_id_serino.id
       """.stripMargin).createOrReplaceTempView("tmp_id_gen")

   // spark.sql("insert overwrite table test.id_new_pre select * from  tmp_id_gen")


    /*spark.sql(
      s"""
         |select imei,mac,serialno,model,min_dt,max_dt,
         | case when id_imei != -1 then id_imei else rand() * -100000 end as id_imei,
         | case when id_mac != -1 then id_mac else rand() * -200000 end as id_mac,
         | case when id_serino != -1 then id_serino else rand() * -300000 end as id_serino,deviceid
         | from  test.id_new_pre
       """.stripMargin).createOrReplaceTempView("tmp_id_gen")*/

    spark.sql(
      s"""
         |select id_imei,sha3(concat(imei,mac,serialno,model)) as deviceid
         |from(
         |select imei,mac,serialno,id_imei,model,row_number() over(partition by imei order by min_dt) as rank
         |from tmp_id_gen
         |where deviceid is null and length(trim(imei))>0 and length(trim(mac))=0 and length(trim(serialno))=0
         |)t1
         |where rank=1
       """.stripMargin).createOrReplaceTempView("tmp_imei")


    spark.sql(
      s"""
         |
         |select id_mac,sha3(concat(imei,mac,serialno,model)) as deviceid
         |from(
         |select imei,mac,serialno,model,min_dt,id_mac,row_number() over(partition by mac order by min_dt) as rank
         |from tmp_id_gen
         |where deviceid is null and length(trim(imei))=0 and length(trim(mac))>0 and length(trim(serialno))=0
         |)t1
         |where rank=1
       """.stripMargin).createOrReplaceTempView("tmp_mac")


    spark.sql(
      s"""
         |select id_serino,sha3(concat(imei,mac,serialno,model)) as deviceid
         |from(
         |select imei,mac,serialno,model,min_dt,id_serino,row_number() over(partition by serialno order by min_dt) as rank
         |from tmp_id_gen
         |where deviceid is null and length(trim(imei))=0 and length(trim(mac))=0 and length(trim(serialno))>0
         |)t1
         |where rank=1
       """.stripMargin).createOrReplaceTempView("tmp_serialno")


    spark.sql(
      s"""
         |select imei,mac,serialno,model,min_dt,max_dt,
         |coalesce(a.deviceid,tmp_imei.deviceid,tmp_mac.deviceid,tmp_serialno.deviceid) as deviceid
         |from tmp_id_gen a
         |left join
         |tmp_imei
         |on a.id_imei = tmp_imei.id_imei
         |left join
         |tmp_mac
         |on a.id_mac = tmp_mac.id_mac
         |left join
         |tmp_serialno
         |on a.id_serino = tmp_serialno.id_serino
       """.stripMargin).createOrReplaceTempView("tmp_rs")

   // spark.sql("create table test.id_new_pre as select * from tmp_rs")
    spark.sql(
      s"""
         |insert overwrite table  test.uni_deviceid_final
         |select imei,mac,serialno,model,min_dt,max_dt,
         |  case when deviceid is null then sha3(concat(imei,mac,serialno,model)) else deviceid end as deviceid,2 as flag
         |from tmp_rs
         |where length(imei)>0 or length(mac)>0 or length(serialno)>0
         |union all
         |select imei,mac,serialno,model,min_dt,max_dt,sha3(concat(imei,mac,serialno,coalesce(model,''))) as deviceid ,flag
         |from test.device_uni_pre
         |where flag = 3
         |union all
         |select imei,mac,serialno,model,min_dt,max_dt,'' as  deviceid ,flag
         |from test.device_uni_pre
         |where flag = 1
         |union all
         |select imei,mac,serialno,model,min_dt,max_dt,
         |   '' as deviceid,1 as flag
         |from tmp_rs
         |where length(imei)=0 and length(mac)=0 and length(serialno)=0
       """.stripMargin)

  }

}
