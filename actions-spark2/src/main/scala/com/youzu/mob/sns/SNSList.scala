package com.youzu.mob.sns

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SNSList {
  def unapply(x: SNSList): Option[SNSList] =
    Some(x)

  def apply(snsplat_1: String, snsplat_2: String, snsplat_3: String, snsplat_4: String, snsplat_5: String, snsplat_6: String, snsplat_7: String, snsplat_8: String, snsplat_9: String, snsplat_10: String, snsplat_11: String, snsplat_12: String, snsplat_13: String, snsplat_14: String, snsplat_15: String, snsplat_16: String, snsplat_17: String, snsplat_18: String, snsplat_19: String, snsplat_20: String, snsplat_21: String, snsplat_22: String, snsplat_23: String, snsplat_24: String, snsplat_25: String, snsplat_26: String, snsplat_27: String, snsplat_28: String, snsplat_29: String, snsplat_30: String, snsplat_31: String, snsplat_32: String, snsplat_33: String, snsplat_34: String, snsplat_35: String, snsplat_36: String, snsplat_37: String, snsplat_38: String, snsplat_39: String, snsplat_40: String, snsplat_41: String, snsplat_42: String, snsplat_43: String, snsplat_44: String, snsplat_45: String, snsplat_46: String, snsplat_47: String, snsplat_48: String, snsplat_49: String, snsplat_50: String, snsplat_51: String, snsplat_52: String, snsplat_53: String, snsplat_54: String): SNSList = new SNSList(snsplat_1, snsplat_2, snsplat_3, snsplat_4, snsplat_5, snsplat_6, snsplat_7, snsplat_8, snsplat_9, snsplat_10, snsplat_11, snsplat_12, snsplat_13, snsplat_14, snsplat_15, snsplat_16, snsplat_17, snsplat_18, snsplat_19, snsplat_20, snsplat_21, snsplat_22, snsplat_23, snsplat_24, snsplat_25, snsplat_26, snsplat_27, snsplat_28, snsplat_29, snsplat_30, snsplat_31, snsplat_32, snsplat_33, snsplat_34, snsplat_35, snsplat_36, snsplat_37, snsplat_38, snsplat_39, snsplat_40, snsplat_41, snsplat_42, snsplat_43, snsplat_44, snsplat_45, snsplat_46, snsplat_47, snsplat_48, snsplat_49, snsplat_50, snsplat_51, snsplat_52, snsplat_53, snsplat_54)

  val snsuidList: StructType = StructType(
    Array(
      StructField("snsplat_1", StringType, nullable = true),
      StructField("snsplat_2", StringType, nullable = true),
      StructField("snsplat_3", StringType, nullable = true),
      StructField("snsplat_4", StringType, nullable = true),
      StructField("snsplat_5", StringType, nullable = true),
      StructField("snsplat_6", StringType, nullable = true),
      StructField("snsplat_7", StringType, nullable = true),
      StructField("snsplat_8", StringType, nullable = true),
      StructField("snsplat_9", StringType, nullable = true),
      StructField("snsplat_10", StringType, nullable = true),
      StructField("snsplat_11", StringType, nullable = true),
      StructField("snsplat_12", StringType, nullable = true),
      StructField("snsplat_13", StringType, nullable = true),
      StructField("snsplat_14", StringType, nullable = true),
      StructField("snsplat_15", StringType, nullable = true),
      StructField("snsplat_16", StringType, nullable = true),
      StructField("snsplat_17", StringType, nullable = true),
      StructField("snsplat_18", StringType, nullable = true),
      StructField("snsplat_19", StringType, nullable = true),
      StructField("snsplat_20", StringType, nullable = true),
      StructField("snsplat_21", StringType, nullable = true),
      StructField("snsplat_22", StringType, nullable = true),
      StructField("snsplat_23", StringType, nullable = true),
      StructField("snsplat_24", StringType, nullable = true),
      StructField("snsplat_25", StringType, nullable = true),
      StructField("snsplat_26", StringType, nullable = true),
      StructField("snsplat_27", StringType, nullable = true),
      StructField("snsplat_28", StringType, nullable = true),
      StructField("snsplat_29", StringType, nullable = true),
      StructField("snsplat_30", StringType, nullable = true),
      StructField("snsplat_31", StringType, nullable = true),
      StructField("snsplat_32", StringType, nullable = true),
      StructField("snsplat_33", StringType, nullable = true),
      StructField("snsplat_34", StringType, nullable = true),
      StructField("snsplat_35", StringType, nullable = true),
      StructField("snsplat_36", StringType, nullable = true),
      StructField("snsplat_37", StringType, nullable = true),
      StructField("snsplat_38", StringType, nullable = true),
      StructField("snsplat_39", StringType, nullable = true),
      StructField("snsplat_40", StringType, nullable = true),
      StructField("snsplat_41", StringType, nullable = true),
      StructField("snsplat_42", StringType, nullable = true),
      StructField("snsplat_43", StringType, nullable = true),
      StructField("snsplat_44", StringType, nullable = true),
      StructField("snsplat_45", StringType, nullable = true),
      StructField("snsplat_46", StringType, nullable = true),
      StructField("snsplat_47", StringType, nullable = true),
      StructField("snsplat_48", StringType, nullable = true),
      StructField("snsplat_49", StringType, nullable = true),
      StructField("snsplat_50", StringType, nullable = true),
      StructField("snsplat_51", StringType, nullable = true),
      StructField("snsplat_52", StringType, nullable = true),
      StructField("snsplat_53", StringType, nullable = true),
      StructField("snsplat_54", StringType, nullable = true)))

  def array2SNSList(array: Array[String]): SNSList = {
    array2SNSList(array, 1)
  }

  def array2SNSList(array: Array[String], offset: Int): SNSList = {
    SNSList(array(offset + 0), array(offset + 1), array(offset + 2), array(offset + 3), array(offset + 4), array(offset + 5), array(offset + 6), array(offset + 7), array(offset + 8), array(offset + 9), array(offset + 10), array(offset + 11), array(offset + 12), array(offset + 13), array(offset + 14), array(offset + 15), array(offset + 16), array(offset + 17), array(offset + 18), array(offset + 19), array(offset + 20), array(offset + 21), array(offset + 22), array(offset + 23), array(offset + 24), array(offset + 25), array(offset + 26), array(offset + 27), array(offset + 28), array(offset + 29), array(offset + 30), array(offset + 31), array(offset + 32), array(offset + 33), array(offset + 34), array(offset + 35), array(offset + 36), array(offset + 37), array(offset + 38), array(offset + 39), array(offset + 40), array(offset + 41), array(offset + 42), array(offset + 43), array(offset + 44), array(offset + 45), array(offset + 46), array(offset + 47), array(offset + 48), array(offset + 49), array(offset + 50), array(offset + 51), array(offset + 52), array(offset + 53))
  }

  def containsUID(field: String, uid: String): Boolean = {
    field.split(",").map(_.split("=")(0)).contains(uid)
  }
}

class SNSList(val snsplat_1: String, val snsplat_2: String, val snsplat_3: String, val snsplat_4: String, val snsplat_5: String, val snsplat_6: String, val snsplat_7: String, val snsplat_8: String, val snsplat_9: String, val snsplat_10: String, val snsplat_11: String, val snsplat_12: String, val snsplat_13: String, val snsplat_14: String, val snsplat_15: String, val snsplat_16: String, val snsplat_17: String, val snsplat_18: String, val snsplat_19: String, val snsplat_20: String, val snsplat_21: String, val snsplat_22: String, val snsplat_23: String, val snsplat_24: String, val snsplat_25: String, val snsplat_26: String, val snsplat_27: String, val snsplat_28: String, val snsplat_29: String, val snsplat_30: String, val snsplat_31: String, val snsplat_32: String, val snsplat_33: String, val snsplat_34: String, val snsplat_35: String, val snsplat_36: String, val snsplat_37: String, val snsplat_38: String, val snsplat_39: String, val snsplat_40: String, val snsplat_41: String, val snsplat_42: String, val snsplat_43: String, val snsplat_44: String, val snsplat_45: String, val snsplat_46: String, val snsplat_47: String, val snsplat_48: String, val snsplat_49: String, val snsplat_50: String, val snsplat_51: String, val snsplat_52: String, val snsplat_53: String, val snsplat_54: String) extends Product with Serializable {

  @throws(classOf[IndexOutOfBoundsException])
  override def productElement(n: Int) = n match {
    case 0 => snsplat_1
    case 1 => snsplat_2
    case 2 => snsplat_3
    case 3 => snsplat_4
    case 4 => snsplat_5
    case 5 => snsplat_6
    case 6 => snsplat_7
    case 7 => snsplat_8
    case 8 => snsplat_9
    case 9 => snsplat_10
    case 10 => snsplat_11
    case 11 => snsplat_12
    case 12 => snsplat_13
    case 13 => snsplat_14
    case 14 => snsplat_15
    case 15 => snsplat_16
    case 16 => snsplat_17
    case 17 => snsplat_18
    case 18 => snsplat_19
    case 19 => snsplat_20
    case 20 => snsplat_21
    case 21 => snsplat_22
    case 22 => snsplat_23
    case 23 => snsplat_24
    case 24 => snsplat_25
    case 25 => snsplat_26
    case 26 => snsplat_27
    case 27 => snsplat_28
    case 28 => snsplat_29
    case 29 => snsplat_30
    case 30 => snsplat_31
    case 31 => snsplat_32
    case 32 => snsplat_33
    case 33 => snsplat_34
    case 34 => snsplat_35
    case 35 => snsplat_36
    case 36 => snsplat_37
    case 37 => snsplat_38
    case 38 => snsplat_39
    case 39 => snsplat_40
    case 40 => snsplat_41
    case 41 => snsplat_42
    case 42 => snsplat_43
    case 43 => snsplat_44
    case 44 => snsplat_45
    case 45 => snsplat_46
    case 46 => snsplat_47
    case 47 => snsplat_48
    case 48 => snsplat_49
    case 49 => snsplat_50
    case 50 => snsplat_51
    case 51 => snsplat_52
    case 52 => snsplat_53
    case 53 => snsplat_54
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  override def productArity = 54

//  override def toString: String = {
//    "(" + snsplat_1 + "," + snsplat_2 + "," + snsplat_3 + "," + snsplat_4 + "," + snsplat_5 + "," + snsplat_6 + "," + snsplat_7 + "," + snsplat_8 + "," + snsplat_9 + "," + snsplat_10 + "," + snsplat_11 + "," + snsplat_12 + "," + snsplat_13 + "," + snsplat_14 + "," + snsplat_15 + "," + snsplat_16 + "," + snsplat_17 + "," + snsplat_18 + "," + snsplat_19 + "," + snsplat_20 + "," + snsplat_21 + "," + snsplat_22 + "," + snsplat_23 + "," + snsplat_24 + "," + snsplat_25 + "," + snsplat_26 + "," + snsplat_27 + "," + snsplat_28 + "," + snsplat_29 + "," + snsplat_30 + "," + snsplat_31 + "," + snsplat_32 + "," + snsplat_33 + "," + snsplat_34 + "," + snsplat_35 + "," + snsplat_36 + "," + snsplat_37 + "," + snsplat_38 + "," + snsplat_39 + "," + snsplat_40 + "," + snsplat_41 + "," + snsplat_42 + "," + snsplat_43 + "," + snsplat_44 + "," + snsplat_45 + "," + snsplat_46 + "," + snsplat_47 + "," + snsplat_48 + "," + snsplat_49 + "," + snsplat_50 + "," + snsplat_51 + "," + snsplat_52 + "," + snsplat_53 + "," + snsplat_54 + ")"
//  }

  override def canEqual(that: Any): Boolean = {
    that match {
      case x: SNSList =>
        compareSnsplatInfo(x.snsplat_1, snsplat_1) &&
          compareSnsplatInfo(x.snsplat_2, snsplat_2) &&
          compareSnsplatInfo(x.snsplat_3, snsplat_3) &&
          compareSnsplatInfo(x.snsplat_4, snsplat_4) &&
          compareSnsplatInfo(x.snsplat_5, snsplat_5) &&
          compareSnsplatInfo(x.snsplat_6, snsplat_6) &&
          compareSnsplatInfo(x.snsplat_7, snsplat_7) &&
          compareSnsplatInfo(x.snsplat_8, snsplat_8) &&
          compareSnsplatInfo(x.snsplat_9, snsplat_9) &&
          compareSnsplatInfo(x.snsplat_10,  snsplat_10) &&
          compareSnsplatInfo(x.snsplat_11,  snsplat_11) &&
          compareSnsplatInfo(x.snsplat_12,  snsplat_12) &&
          compareSnsplatInfo(x.snsplat_13,  snsplat_13) &&
          compareSnsplatInfo(x.snsplat_14,  snsplat_14) &&
          compareSnsplatInfo(x.snsplat_15,  snsplat_15) &&
          compareSnsplatInfo(x.snsplat_16,  snsplat_16) &&
          compareSnsplatInfo(x.snsplat_17,  snsplat_17) &&
          compareSnsplatInfo(x.snsplat_18,  snsplat_18) &&
          compareSnsplatInfo(x.snsplat_19,  snsplat_19) &&
          compareSnsplatInfo(x.snsplat_20,  snsplat_20) &&
          compareSnsplatInfo(x.snsplat_21,  snsplat_21) &&
          compareSnsplatInfo(x.snsplat_22,  snsplat_22) &&
          compareSnsplatInfo(x.snsplat_23,  snsplat_23) &&
          compareSnsplatInfo(x.snsplat_24,  snsplat_24) &&
          compareSnsplatInfo(x.snsplat_25,  snsplat_25) &&
          compareSnsplatInfo(x.snsplat_26,  snsplat_26) &&
          compareSnsplatInfo(x.snsplat_27,  snsplat_27) &&
          compareSnsplatInfo(x.snsplat_28,  snsplat_28) &&
          compareSnsplatInfo(x.snsplat_29,  snsplat_29) &&
          compareSnsplatInfo(x.snsplat_30,  snsplat_30) &&
          compareSnsplatInfo(x.snsplat_31,  snsplat_31) &&
          compareSnsplatInfo(x.snsplat_32,  snsplat_32) &&
          compareSnsplatInfo(x.snsplat_33,  snsplat_33) &&
          compareSnsplatInfo(x.snsplat_34,  snsplat_34) &&
          compareSnsplatInfo(x.snsplat_35,  snsplat_35) &&
          compareSnsplatInfo(x.snsplat_36,  snsplat_36) &&
          compareSnsplatInfo(x.snsplat_37,  snsplat_37) &&
          compareSnsplatInfo(x.snsplat_38,  snsplat_38) &&
          compareSnsplatInfo(x.snsplat_39,  snsplat_39) &&
          compareSnsplatInfo(x.snsplat_40,  snsplat_40) &&
          compareSnsplatInfo(x.snsplat_41,  snsplat_41) &&
          compareSnsplatInfo(x.snsplat_42,  snsplat_42) &&
          compareSnsplatInfo(x.snsplat_43,  snsplat_43) &&
          compareSnsplatInfo(x.snsplat_44,  snsplat_44) &&
          compareSnsplatInfo(x.snsplat_45,  snsplat_45) &&
          compareSnsplatInfo(x.snsplat_46,  snsplat_46) &&
          compareSnsplatInfo(x.snsplat_47,  snsplat_47) &&
          compareSnsplatInfo(x.snsplat_48,  snsplat_48) &&
          compareSnsplatInfo(x.snsplat_49,  snsplat_49) &&
          compareSnsplatInfo(x.snsplat_50,  snsplat_50) &&
          compareSnsplatInfo(x.snsplat_51,  snsplat_51) &&
          compareSnsplatInfo(x.snsplat_52,  snsplat_52) &&
          compareSnsplatInfo(x.snsplat_53,  snsplat_53) &&
          compareSnsplatInfo(x.snsplat_54,  snsplat_54)

      case _ => false
    }
  }

  override def equals(that: scala.Any): Boolean = {
    that match {
      case that: SNSList => that.canEqual(this)
      case _ => false
    }
  }

  private def compareSnsplatInfo(s1: String, s2: String): Boolean = {
    (s1.split(",") diff s2.split(",")).isEmpty
  }
}