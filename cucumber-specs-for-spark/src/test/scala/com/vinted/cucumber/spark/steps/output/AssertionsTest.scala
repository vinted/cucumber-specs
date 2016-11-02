package com.vinted.cucumber.spark.steps.output

import com.vinted.cucumber.spark.spec.TestHive.implicits._
import com.vinted.cucumber.spark.ext.DataFrameExt._
import org.scalatest.exceptions.TestFailedException
import org.scalatest.{FunSpec, Matchers}

class AssertionsTest extends FunSpec with Matchers {
  describe("#assertIsSubset") {
    val expected = Seq(
      (101, 102, 103),
      (111, 112, 113),
      (121, 122, 123)
    ).toDF("col1", "col2", "col3")

    it("does not fail for exact match") {
      new { } with Assertions {
        noException should be thrownBy assertIsSubset(expected, expected)
      }
    }

    it("does not fail due to excess rows") {
      new { } with Assertions {
        val actualSuperset = Seq(
          (91, 92, 93),
          (101, 102, 103),
          (111, 112, 113),
          (121, 122, 123),
          (131, 132, 133)
        ).toDF("col1", "col2", "col3")
        noException should be thrownBy assertIsSubset(expected, actualSuperset)
      }
    }

    it("does not fail due to excess columns") {
      new { } with Assertions {
        val actualSuperset = Seq(
          (101, 102, 103, 104),
          (111, 112, 113, 114),
          (121, 122, 123, 124)
        ).toDF("col1", "col2", "col3", "col4")
        noException should be thrownBy assertIsSubset(expected, actualSuperset)
      }
    }

    it("fails for mismatching subset") {
      val actualWithMismatchingSubset = Seq(
        (101, 102, 103),
        (111, 666, 113),
        (121, 122, 123)
      ).toDF("col1", "col2", "col3")

      new { } with Assertions {
        intercept[TestFailedException] {
          assertIsSubset(expected, actualWithMismatchingSubset)
        }.getMessage.trim shouldBe """
Unexpected result:
Difference:
+------+-------------+------+
| col1 | col2        | col3 |
+------+-------------+------+
| 101  | 102         | 103  |
| 111  | 112 -/+ 666 | 113  |
| 121  | 122         | 123  |
+------+-------------+------+

Expected:
+------+------+------+
| col1 | col2 | col3 |
+------+------+------+
| 101  | 102  | 103  |
| 111  | 112  | 113  |
| 121  | 122  | 123  |
+------+------+------+

Actual:
+------+------+------+
| col1 | col2 | col3 |
+------+------+------+
| 101  | 102  | 103  |
| 111  | 666  | 113  |
| 121  | 122  | 123  |
+------+------+------+
""".trim
      }
    }

    it("fails for missing row") {
      val actualWithMismatchingSubset = Seq(
        (101, 102, 103),
        (121, 122, 123)
      ).toDF("col1", "col2", "col3")

      new { } with Assertions {
        intercept[TestFailedException] {
          assertIsSubset(expected, actualWithMismatchingSubset)
        }.getMessage.trim shouldBe """
Unexpected result:
Difference:
+-------------+-------------+-------------+
| col1        | col2        | col3        |
+-------------+-------------+-------------+
| 101         | 102         | 103         |
| 111 -/+ 101 | 112 -/+ 102 | 113 -/+ 103 |
| 121         | 122         | 123         |
+-------------+-------------+-------------+

Expected:
+------+------+------+
| col1 | col2 | col3 |
+------+------+------+
| 101  | 102  | 103  |
| 111  | 112  | 113  |
| 121  | 122  | 123  |
+------+------+------+

Actual:
+------+------+------+
| col1 | col2 | col3 |
+------+------+------+
| 101  | 102  | 103  |
| 121  | 122  | 123  |
+------+------+------+
""".trim
      }
    }

    it("fails for empty table") {
      val emptyTable = Seq[Tuple3[Long,Long,Long]]().toDF("col1", "col2", "col3")

      new { } with Assertions {
        intercept[TestFailedException] {
          assertIsSubset(expected, emptyTable)
        }.getMessage.trim shouldBe """
Unexpected result:
Difference:
+--------------+--------------+--------------+
| col1         | col2         | col3         |
+--------------+--------------+--------------+
| 101 -/+ null | 102 -/+ null | 103 -/+ null |
| 111 -/+ null | 112 -/+ null | 113 -/+ null |
| 121 -/+ null | 122 -/+ null | 123 -/+ null |
+--------------+--------------+--------------+

Expected:
+------+------+------+
| col1 | col2 | col3 |
+------+------+------+
| 101  | 102  | 103  |
| 111  | 112  | 113  |
| 121  | 122  | 123  |
+------+------+------+

Actual:
*Empty table*
""".trim
      }
    }

    it("fails for missing column") {
      val actualWithMissingColumn = Seq(Tuple2(1, 2)).toDF("col1", "col2")

      new { } with Assertions {
        intercept[TestFailedException] {
          assertIsSubset(expected, actualWithMissingColumn)
        }.getMessage shouldBe """Array("col1", "col2") did not contain element "col3""""
      }
    }
  }

  describe("#assertNoIntersection") {
    val expectedToBeAbsent = Seq(
      (101, 102, 103),
      (111, 112, 113)
    ).toDF("col1", "col2", "col3")

    it("does not fail for mismatch") {
      new { } with Assertions {
        val actual = Seq((2016, 12, 31)).toDF("col1", "col2", "col3")
        noException should be thrownBy assertNoIntersection(expectedToBeAbsent, actual)
      }
    }

    it("does not fail due to excess columns") {
      new { } with Assertions {
        val actual = Seq(
          (2016, 12, 31, 59)
        ).toDF("col1", "col2", "col3", "col4")
        noException should be thrownBy assertNoIntersection(expectedToBeAbsent, actual)
      }
    }

    it("does not fail for empty table") {
      val emptyTable = Seq[Tuple3[Long,Long,Long]]().toDF("col1", "col2", "col3")

      new { } with Assertions {
        noException should be thrownBy assertNoIntersection(expectedToBeAbsent, emptyTable)
      }
    }

    it("fails for matching subset") {
      val actualWithMatchingSubset = Seq(
        (301, 302, 303),
        (111, 112, 113),
        (401, 402, 403)
      ).toDF("col1", "col2", "col3")

      new { } with Assertions {
        intercept[TestFailedException] {
          assertNoIntersection(expectedToBeAbsent, actualWithMatchingSubset)
        }.getMessage.trim shouldBe """
Unexpected rows found:
+------+------+------+
| col1 | col2 | col3 |
+------+------+------+
| 111  | 112  | 113  |
+------+------+------+

In:
+------+------+------+
| col1 | col2 | col3 |
+------+------+------+
| 301  | 302  | 303  |
| 111  | 112  | 113  |
| 401  | 402  | 403  |
+------+------+------+
""".trim
      }
    }

    it("fails for missing column") {
      val actualWithMissingColumn = Seq(Tuple2(1, 2)).toDF("col1", "col2")

      new { } with Assertions {
        intercept[TestFailedException] {
          assertNoIntersection(expectedToBeAbsent, actualWithMissingColumn)
        }.getMessage shouldBe """Array("col1", "col2") did not contain element "col3""""
      }
    }
  }
}
