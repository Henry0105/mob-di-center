package com.mob

import org.apache.commons.lang.StringUtils

package object mid_full {

    // udf1
    def app_fingerprint(pkg: String, time: String): String = {
        if (StringUtils.isNotBlank(pkg)
                && StringUtils.isNotBlank(time)
                && time.length == 13
                && !time.endsWith("000")) {
            pkg + "_" + time
        } else null
    }

    // udf2
    def openid_resembled(ids: Seq[String]): Seq[(String, String, Int)] = {
        import scala.collection.mutable.ArrayBuffer
        val openidList = ids.toList
        val out = new ArrayBuffer[(String, String, Int)]()
        for (i <- openidList.indices) {
            val sourceOpenid = openidList(i)
            for (j <- i + 1 until openidList.size) {
                val targetOpenid = openidList(j)
                if (sourceOpenid.compareTo(targetOpenid) > 0) {
                    out += ((sourceOpenid, targetOpenid, 1))
                } else {
                    out += ((targetOpenid, sourceOpenid, 1))
                }
            }
        }
        out
    }
    def getPkgVersion(pkg_it: String): String = {
        StringUtils.substringBeforeLast(pkg_it, "_")
    }
}
