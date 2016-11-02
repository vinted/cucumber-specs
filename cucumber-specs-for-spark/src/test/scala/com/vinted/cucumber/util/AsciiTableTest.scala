package com.vinted.cucumber.util

import org.scalatest.{FunSpec, Matchers}

class AsciiTableTest extends FunSpec with Matchers {
  describe("#apply") {
    it("returns printable ASCII table string") {
      AsciiTable(
        Seq("col1", "col2"),
        Seq(
          Seq("one", "two"),
          Seq(1, 2),
          Seq(
            "this value is very long and has lots of spaces",
            "this value is also very long and also has lots of s p a c e s"
          )
        )
      ).trim shouldBe """
+------------------------------------------------+---------------------------------------------------------------+
| col1                                           | col2                                                          |
+------------------------------------------------+---------------------------------------------------------------+
| one                                            | two                                                           |
| 1                                              | 2                                                             |
| this value is very long and has lots of spaces | this value is also very long and also has lots of s p a c e s |
+------------------------------------------------+---------------------------------------------------------------+
""".trim
    }

    it("throws exception if at least one row is too long") {
      intercept[RuntimeException] {
        AsciiTable(
          Seq("col1", "col2"),
          Seq(
            Seq("one", "two"),
            Seq("one", "two", "three")
          )
        )
      }.getMessage shouldBe "Unexpected row width: 3 vs expected 2"
    }

    it("throws exception if at least one row is too short") {
      intercept[RuntimeException] {
        AsciiTable(
          Seq("col1", "col2"),
          Seq(
            Seq("one", "two"),
            Seq("one")
          )
        )
      }.getMessage shouldBe "Unexpected row width: 1 vs expected 2"
    }
  }
}
