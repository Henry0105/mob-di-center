package com.contacts.etl
import java.util.regex.{Matcher, Pattern}
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkConf, SparkContext}
import scala.collection.mutable.ArrayBuffer
import scala.util.parsing.json.JSON

case class Person(appkey: String,
  duid: String, my_phone: String, zone: String, simserial: String, operator: String, serverTime: String)

case class Phones(displayname: String, position: String, company: String, phone: String)

case class ContactsPhones(duid: String, my_phone: String, phones: Seq[Phones])

case class PhonesMapping(phoneId: String, phoneNum: Seq[String], displayname: String, company: String, position: String)

object contactsEtl extends Logging {

  def formatJsonStr(str: String): String = {
    val m: Matcher = Pattern.compile("^\\w.*\\s\\{").matcher(str)
    val arr = ArrayBuffer[String]()
    while (m.find()) {
      arr += m.group()
    }
    val newStr = "{\"serverTime\":\"" +
      arr(0).replaceAll("\\{", "") + "\"," +
      str.replaceAll("^\\w.*\\s\\{", "")
    newStr
  }

  def formatPhone(zone: String, phone: String): String = {

    val phoneRegex = "^\\+?((0)|(86)|(086)|(860)|(0086)|(125831)|(12583)|(12520)|(17951)|(12593))"
    if (phone == null || phone.length < 1) ""
    else StringUtils.leftPad(zone, 5, "0") + StringUtils.leftPad(phone.replaceAll(phoneRegex, ""), 12, "0")
  }

  def transToPhonePersonInfo(rawStr: String): String = {
    val mapper = new ObjectMapper with Serializable
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    try {
      val json: Option[Any] = JSON.parseFull(rawStr)
      val perContacts = json.get.asInstanceOf[Map[String, Any]]
      val appkey = perContacts.get("appkey").getOrElse("").toString
      val duid = perContacts.get("duid").getOrElse("").toString
      val my_phone = perContacts.get("my_phone").getOrElse("").toString
      val zone = perContacts.get("zone").getOrElse("").toString
      val simserial = perContacts.get("simserial").getOrElse("").toString
      val operator = perContacts.get("operator").getOrElse("").toString
      val serverTime = perContacts.get("serverTime").getOrElse("").toString
      mapper.writeValueAsString(Person(appkey, duid, my_phone, zone, simserial, operator, serverTime))
    } catch {
      case _: Throwable =>
        mapper.writeValueAsString(Person)
    }
  }

  def transToPhoneContacts(rawStr: String): String = {
    val mapper = new ObjectMapper with Serializable
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    try {
      val json: Option[Any] = JSON.parseFull(rawStr)
      val perContacts = json.get.asInstanceOf[Map[String, Any]]
      val contacts = perContacts.get("contacts").get.asInstanceOf[List[Map[String, Any]]]
      val newContacts = contacts.map {
        r =>
          val displayname = r.get("displayname").getOrElse("").toString
          val company = r.get("position").getOrElse("").toString
          val position = r.get("company").getOrElse("").toString
          val phonesStruct = r.get("phones").get.asInstanceOf[List[Map[String, Any]]]
          val phones = phonesStruct.map(r => r.get("phone").getOrElse("").toString)
          Phones(displayname, company, position, phones.head)
      }
      val newPhoneStructRes = ContactsPhones(
        perContacts.get("duid").getOrElse("").toString,
        perContacts.get("my_phone").getOrElse("").toString,
        newContacts.toSeq
      )
      mapper.writeValueAsString(newPhoneStructRes)
    } catch {
      case _: Throwable =>
        mapper.writeValueAsString(ContactsPhones)
    }
  }

  def transToPhoneMapping(rawStr: String): String = {
    val mapper = new ObjectMapper with Serializable
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    mapper.registerModule(DefaultScalaModule)
    try {
      val json: Option[Any] = JSON.parseFull(rawStr)
      val perContacts = json.get.asInstanceOf[Map[String, Any]]
      val contacts = perContacts.get("contacts").get.asInstanceOf[List[Map[String, Any]]]

      val newContacts = contacts.map {
        r =>
          val displayname = r.get("displayname").getOrElse("").toString
          val company = r.get("company").getOrElse("").toString
          val position = r.get("position").getOrElse("").toString
          val phonesStruct = r.get("phones").get.asInstanceOf[List[Map[String, Any]]]
          val phones = phonesStruct.map(r => r.get("phone").getOrElse("").toString)
          PhonesMapping(phones.head, phones.toSeq, displayname, company, position)
      }
      mapper.writeValueAsString(newContacts)
    } catch {
      case _: Throwable =>
        mapper.writeValueAsString(PhonesMapping)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("yarn-client")
    val sc = new SparkContext(conf)
    val sqlContext = new HiveContext(sc)

    val fileInputPath = args(0)
    val date = args(1)
    val hour = args(2)
    val dataBase = args(3)

    val phonePersonInfoTableName = "phone_person_info"
    val phoneContactsTableName = "phone_contacts"
    val phoneInfoMappingTableName = "phone_info_mapping"

    //    val fileInputPath="/user/dba/flumelog/smssdk/contacts/2.0/dt=201610*/*/*.lzo"
    logInfo("FileInputPath IS " + fileInputPath)

    val log = sc.textFile(fileInputPath)

    if (log.isEmpty()) {
      logInfo("该批次数据为空！")
      sc.stop()
    } else {
      val rdd = log.filter(_.isEmpty == false).map(r =>
        r.replaceAll("\\u0001", "")
          .replaceAll("\\\\r|\\\\n", "")
          .replaceAll("[\\n\\r\\u0085\\u2028\\u2029]", "")
      ).map(r => contactsEtl.formatJsonStr(r)).repartition(1500).cache()

      val phonePersonInfoRdd = rdd.map(r => contactsEtl.transToPhonePersonInfo(r))
      val phonePersonInfoDF = sqlContext.read.json(phonePersonInfoRdd.toJavaRDD)
      val phonePersonInfoDfFilter = phonePersonInfoDF.where(phonePersonInfoDF.col("duid") !== "")
      phonePersonInfoDfFilter.distinct().registerTempTable("phonePersonInfoDfFilter")
      sqlContext.sql("INSERT OVERWRITE TABLE "
        + dataBase + "." + phonePersonInfoTableName +
        " partition (dt=" + date + ",hour=" + hour + ") " +
        "SELECT appkey AS appkey, duid AS duid, my_phone AS my_phone, zone AS zone," +
        " simserial AS simserial,operator AS operator,serverTime AS serverTime  " +
        "FROM phonePersonInfoDfFilter"
      )

      val phoneContactsRdd = rdd.map(r => contactsEtl.transToPhoneContacts(r))
      val phoneContactsDF = sqlContext.read.json(phoneContactsRdd.toJavaRDD)
      val phoneContactsDfFilter = phoneContactsDF.where(phoneContactsDF.col("duid") !== "")
      val phoneContactsExplodedDF = phoneContactsDfFilter
        .withColumn("phones",
          explode(phoneContactsDfFilter("phones")))
        .select("duid", "my_phone", "phones.displayname", "phones.company", "phones.position", "phones.phone")

      phoneContactsExplodedDF.distinct.registerTempTable("phoneContactsExplodedDF")
      sqlContext.sql("" +
        "INSERT OVERWRITE TABLE " + dataBase + "." +
        phoneContactsTableName + " partition (dt= " + date + ",hour=" + hour + ") " +
        "SELECT duid AS duid, my_phone AS my_phone, displayname AS displayname, " +
        "company AS company, position AS position, phone AS phone  " +
        "FROM phoneContactsExplodedDF")


      val phoneMappingRdd = rdd.map(r => contactsEtl.transToPhoneMapping(r))
      val phoneMappingDF = sqlContext.read.json(phoneMappingRdd.toJavaRDD)
      val phoneMappingDfFilter = phoneMappingDF.where(phoneMappingDF.col("phoneId") !== "")
      val phoneMappingExplodedDF = phoneMappingDfFilter
        .withColumn("phoneNum", explode(phoneMappingDfFilter("phoneNum")))
        .select("displayname", "phoneId", "phoneNum", "company", "position")

      phoneMappingExplodedDF.distinct()
        .registerTempTable("phoneMappingExplodedDF")

      sqlContext.sql(
        "INSERT OVERWRITE TABLE " + dataBase + "." + phoneInfoMappingTableName +
          " partition (dt=" + date + ",hour=" + hour + ") " +
          "SELECT phoneId AS phone_id, phoneNum AS phone, displayname AS displayname, " +
          "company AS company, position AS position  " +
          "FROM phoneMappingExplodedDF"
      )
      sc.stop()
    }
  }
}
