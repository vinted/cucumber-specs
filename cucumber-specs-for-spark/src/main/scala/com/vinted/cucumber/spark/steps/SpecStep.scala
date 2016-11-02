package com.vinted.cucumber.spark.steps

import com.vinted.cucumber.spark.spec.Context
import cucumber.api.scala.{ScalaDsl, EN}
import org.scalatest.Matchers

trait SpecStep extends ScalaDsl with EN with Matchers {
  lazy val context = Context
  lazy val sql = context.sqlContext.sql _
}
