package com.mob.deviceid.udf

class ImeiVerifyUDF {

  def ieid_verify(arg : String): Boolean = {
    ImeiVerify.evaluate(arg)
  }


}
