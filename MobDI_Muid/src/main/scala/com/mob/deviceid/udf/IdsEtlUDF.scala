package com.mob.deviceid.udf

class IdsEtlUDF {

  def ifIdsValHasMore(ieid: String, mcid: String, snid: String, oiid: String, asid: String): Boolean = {
    val ids = List(ieid, mcid, snid, oiid, asid)
    var list = List[String]()
    for (id <- ids) {
      if (id != null && id != "") {
        list = list :+ id
      }
    }
    list.size > 1
  }

  def ifIdsValOnlyOne(ieid: String, mcid: String, snid: String, oiid: String, asid: String): Boolean = {
    val ids = List(ieid, mcid, snid, oiid, asid)
    var list = List[String]()
    for (id <- ids) {
      if (id != null && id != "") {
        list = list :+ id
      }
    }
    list.size <= 1
  }

}
