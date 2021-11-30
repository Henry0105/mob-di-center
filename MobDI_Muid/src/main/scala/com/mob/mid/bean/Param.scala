package com.mob.mid.bean

case class Param(
                  day: String = "",
                  pkgItLimit: Int = 5000,
                  pkgReinstallTimes: Int = 100,
                  edgeLimit: Int = 7,
                  graphConnectTimes: Int = 10
                )
