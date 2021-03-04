package com.youzu.mob.stall

object LogStallUtil {


  /**
    * dws_device_install_status 状态表中 reserved_flag计算逻辑
    *
    * @return
    */
  def countSum: (String) => String =
    (str: String) => {
      var install_flag = 0
      var unstall_flag = 0
      var flag = true
      val status = str.split(",")
        .map(_.split("=")(1))
        .toList
      for (i <- 1 to status.length - 1 if flag) {
        status(i) match {
          case "1" => {
            install_flag = 1; flag = false
          }
          case "0" => if (status(i - 1) == "-1") {
            install_flag = 1; flag = false
          }
          case "-1" => unstall_flag = 1
          case _ => install_flag; unstall_flag
        }
      }
      if (install_flag > 0) "1"
      else if (unstall_flag > 0) "-1"
      else "0"
    }
}

