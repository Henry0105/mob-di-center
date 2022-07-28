package com.mob.deviceid.udf

class SHA1HashingUDF {

  def sha(arg: String): String = {
    SHA1Hashing.evaluate(arg)
  }

}
