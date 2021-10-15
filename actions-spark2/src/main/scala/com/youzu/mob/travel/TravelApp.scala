package com.youzu.mob.travel

import com.youzu.mob.utils.Constants._

import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

case class CleanedProfileFullRow(
  device: String,
  businessFlag: Boolean,
  car: Boolean,
  cheapFlightInstalled: Boolean,
  flightInstalled: Boolean,
  ticketInstalled: Boolean,
  rentcarInstalled: Boolean)

case class AppActiveRow(
  device: String,
  busiAppAct: Boolean,
  travelAppAct: Boolean,
  flightActive: Boolean,
  ticketActive: Boolean,
  rentcarActive: Boolean)

object TravelApp {
  val CLEANED_PROFILE_FULL_MOBDI = "cleaned_profile_full_mobdi"
  val narrowedAppActiveTable = "narrowed_app_active_mobdi"

  def main(args: Array[String]): Unit = {
    val day = args(0)
    val HOST = args(1)
    val targetTable = args(2)
    val PORT = 9083
    val PROTOCOL = "thrift"
    val URL = s"$PROTOCOL://$HOST:$PORT"

    val spark = SparkSession
      .builder()
      .appName("TravelApp")
      .config("hive.metastore.uris", URL)
      .enableHiveSupport()
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext

    import spark.implicits._

    val pkg2ApppkgMap: Map[String, String] = spark.sql(
      s"select pkg, apppkg from $APP_PKG_MAPPING_PAR where version='1000'"
    ).collect().map(
      r => (r.getAs[String]("pkg"
      ), r.getAs[String]("apppkg"))).toMap

    val cleanedBusiAppAct = sc.broadcast(cleanPkg(Pkgs.busiAppAct, pkg2ApppkgMap))
    val cleanedTravelAppAct = sc.broadcast(cleanPkg(Pkgs.travelAppAct, pkg2ApppkgMap))
    val cleanedCheapFlightInstalled = sc.broadcast(cleanPkg(Pkgs.cheapFlightInstalled, pkg2ApppkgMap))
    val cleanedFlightInstalled = sc.broadcast(cleanPkg(Pkgs.flightInstalled, pkg2ApppkgMap))
    val cleanedFlightActive = sc.broadcast(cleanPkg(Pkgs.flightActive, pkg2ApppkgMap))
    val cleanedTicket = sc.broadcast(cleanPkg(Pkgs.ticket, pkg2ApppkgMap))
    val cleanedRentCar = sc.broadcast(cleanPkg(Pkgs.rentCar, pkg2ApppkgMap))
    val pkg2ApppkgMapBroadcast = sc.broadcast(pkg2ApppkgMap)


    val cleanedProfile: Dataset[CleanedProfileFullRow] = spark.sql(
      s"select device, segment, car, applist " +
        s"from $RP_DEVICE_PROFILE_FULL_VIEW")
      .mapPartitions(
        iter => {
          iter.map(
            row => {
              val applist = row.getAs[String]("applist")
              val cleanedApplist = cleanPkg(applist.split(",").toSet, pkg2ApppkgMapBroadcast.value)
              CleanedProfileFullRow(
                row.getAs[String]("device"),
                29.equals(row.getAs[Int]("segment")),
                1.equals(row.getAs[Int]("car")),
                cleanedApplist.intersect(cleanedCheapFlightInstalled.value).nonEmpty,
                cleanedApplist.intersect(cleanedFlightInstalled.value).nonEmpty,
                cleanedApplist.intersect(cleanedTicket.value).nonEmpty,
                cleanedApplist.intersect(cleanedRentCar.value).nonEmpty
              )
            })
        })

    cleanedProfile.createOrReplaceTempView(CLEANED_PROFILE_FULL_MOBDI)

    val narrowedAppActiveDaily: Dataset[AppActiveRow] = spark.sql(
      s"""
         |select device, apppkg from $APP_ACTIVE_DAILY where day=$day
       """.stripMargin)
      .mapPartitions(
        iter => {
          iter.map(
            row => {
              val apppkg = row.getAs[String]("apppkg")
              val device = row.getAs[String]("device")
              (device,
                AppActiveRow(
                  device,
                  cleanedBusiAppAct.value.contains(apppkg),
                  cleanedTravelAppAct.value.contains(apppkg),
                  cleanedFlightActive.value.contains(apppkg),
                  cleanedTicket.value.contains(apppkg),
                  cleanedRentCar.value.contains(apppkg))
              )
            })
        }).rdd
      .reduceByKey(
        (r1, r2) =>
          AppActiveRow(
            r1.device,
            r1.busiAppAct || r2.busiAppAct,
            r1.travelAppAct || r2.travelAppAct,
            r1.flightActive || r2.flightActive,
            r1.ticketActive || r2.ticketActive,
            r1.rentcarActive || r2.rentcarActive)
      )
      .mapPartitions(iter => iter.map(_._2)).toDS()

    narrowedAppActiveDaily.createOrReplaceTempView(narrowedAppActiveTable)

    val query =
      s"""
         |select
         |    a.device,
         |    a.country,
         |    a.province,
         |    a.city,
         |    a.pcountry,
         |    a.pprovince,
         |    a.pcity,
         |    case when b.device is null then 0
         |    else 1 end
         |    as poi_flag,
         |    c.continents,
         |    c.travel_area,
         |    coalesce(d.flag,0) as province_flag,
         |    coalesce(e.city_level, -1) as pcity_level,
         |    ${getVacationFlag(spark, day)} as vaca_flag,
         |    coalesce(cast(g.businessFlag as int), 0),
         |    coalesce(cast(h.busiAppAct as int), 0),
         |    coalesce(cast(g.car as int), 0),
         |    coalesce(cast(h.travelAppAct as int), 0),
         |    coalesce(cast(g.cheapFlightInstalled as int), 0),
         |    coalesce(cast(g.flightInstalled as int), 0),
         |    coalesce(cast(h.flightActive as int), 0),
         |    coalesce(cast(g.ticketInstalled as int), 0),
         |    coalesce(cast(h.ticketActive as int), 0),
         |    coalesce(cast(h.rentcarActive as int), 0),
         |    coalesce(cast(g.rentcarInstalled as int), 0)
         |from (
         | select device, country, province, city, pcountry,
         |  pprovince, pcity, day from $DWS_DEVICE_TRAVEL_LOCATION_DI
         | where day = $day and (
         |     (country != pcountry and trim(country) != '' and trim(pcountry) != '')
         |     or (province != pprovince and trim(province) != '' and trim(pprovince) != '')
         |     or (city != pcity and trim(city) != '' and trim(pcity) != ''))
         |) a
         |left join
         |(
         |select device,country,province,city from $DWS_DEVICE_LBS_POI_ANDROID_SEC_DI
         |where type = 9 and day = $day group by  device,country,province,city
         |) b
         |on a.device=b.device and a.province=b.province and a.city=b.city
         |left join $MAP_COUNTRY_SDK c
         |on a.country=c.zone
         |left join $MAP_PROVINCE_LOC d
         |on a.province=d.province1_code and a.pprovince=d.province2_code
         |left join $MAP_CITY_SDK e
         |on a.pcity=e.city_code
         |left join $CLEANED_PROFILE_FULL_MOBDI as g
         |on a.device = g.device
         |left join $narrowedAppActiveTable as h
         |on a.device = h.device
      """.stripMargin

    println(query)
    spark.sql(query).explain(true)
    spark.sql(
      s"""
         |insert  overwrite table $targetTable partition (day='$day')
         |$query
      """.stripMargin)
    spark.close()
  }

  def cleanPkg(pkgs: Set[String], mapping: Map[String, String]): Set[String] = {
    pkgs.map(pkg => mapping.getOrElse(pkg, pkg))
  }


  def getVacationFlag(spark: SparkSession, day: String): Int = {
    // 注意: 表变更为分区表了,但是这里取的是第一行并且数据量较小所以不影响
    val rows = spark.sql(s"select flag from $VACATION_FLAG where day=$day").collect()
    if (rows.nonEmpty) {
      rows(0).getInt(0)
    } else {
      val c = Calendar.getInstance()
      c.setTime(new SimpleDateFormat("yyyyMMdd").parse(day))

      c.get(Calendar.DAY_OF_WEEK) match {
        case Calendar.SUNDAY => 4
        case Calendar.SATURDAY => 4
        case _ => 3
      }
    }

  }
}