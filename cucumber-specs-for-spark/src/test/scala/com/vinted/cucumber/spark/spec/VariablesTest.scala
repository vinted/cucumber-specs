package com.vinted.cucumber.spark.spec

import org.scalatest.{FunSpec, Matchers}

class VariablesTest extends FunSpec with Matchers {
  describe("#variables") {
    it("is empty at first") {
      new { } with Variables {
        variables shouldBe empty
      }
    }
  }

  describe("#withVariable") {
    it("adds new variables") {
      new { } with Variables {
        withVariable("var1_name", "var1_value")
        withVariable("var2_name", "var2_value")
        variables shouldBe Map(
          "var1_name" -> "var1_value",
          "var2_name" -> "var2_value"
        )
      }
    }

    it("latest value is stored") {
      new { } with Variables {
        withVariable("var_name", "var_value")
        withVariable("var_name", "var_value_latest")
        variables shouldBe Map(
          "var_name" -> "var_value_latest"
        )
      }
    }
  }

  describe("#replaceVariables") {
    it("replaces known variables") {
      new { } with Variables {
        withVariable("var1_name", "var1_value")
        withVariable("var2_name", "var2_value")
        replaceVariables("string with ${var1_name} and ${var2_name}") shouldBe
          "string with var1_value and var2_value"
      }
    }

    it("throws exception for unknown variables") {
      new { } with Variables {
        intercept[RuntimeException] {
          replaceVariables("string with ${unknown_variable}")
        }.getMessage shouldBe "Undefined variable(s): ${unknown_variable}"
      }
    }
  }

  describe("#resetVariables") {
    it("adds new variables") {
      new { } with Variables {
        withVariable("var1_name", "var1_value")
        withVariable("var2_name", "var2_value")
        resetVariables()
        variables shouldBe empty
      }
    }
  }
}
