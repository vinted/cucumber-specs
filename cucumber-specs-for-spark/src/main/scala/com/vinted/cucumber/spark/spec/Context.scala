package com.vinted.cucumber.spark.spec

object Context extends Variables {
  lazy val sqlContext = TestHive

  def reset() = {
    resetVariables()
    sqlContext.reset()
  }
}
