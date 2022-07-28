package com.mob.deviceid.udf

class LuhnCheckerUDF {

  def luhn_checker(arg : String): Boolean = {
    LuhnChecker.evaluate(Array(arg))
  }

}
