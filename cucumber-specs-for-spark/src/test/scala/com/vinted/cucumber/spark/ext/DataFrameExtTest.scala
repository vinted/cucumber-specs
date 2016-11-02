package com.vinted.cucumber.spark.ext

import com.vinted.cucumber.spark.spec.TestHive.implicits._
import com.vinted.cucumber.spark.ext.DataFrameExt._
import org.apache.spark.sql.AnalysisException
import org.scalatest.{FunSpec, Matchers}
import java.lang.Long

class DataFrameExtTest extends FunSpec with Matchers {
  describe("#toAscii") {
    it("returns ASCII table string") {
      Seq((1, 2, 3), (4, 5, 6)).toDF("col1", "col2", "col3").toAscii().trim shouldBe """
+------+------+------+
| col1 | col2 | col3 |
+------+------+------+
| 1    | 2    | 3    |
| 4    | 5    | 6    |
+------+------+------+
""".trim
    }
  }

  describe("#diff") {
    val emptyTable = Seq[Tuple3[Long,Long,String]]().toDF("col1", "col2", "col3")

    val tableA = Seq(
      (101, 102, "103"),
      (111, 112, "113"),
      (121, 122, "123")
    ).toDF("col1", "col2", "col3")

    val tableB = Seq[Tuple3[Long,Long,String]](
      (201, 102, "103"),
      (111, null, null),
      (121, 122, "123")
    ).toDF("col1", "col2", "col3")

    val totallyDifferentTable = Seq(
      (Tuple1("foo"))
    ).toDF("bar")

    it("returns stringy diff (tableA vs tableB)") {
      tableA.diff(tableB).trim shouldBe """
+-------------+--------------+----------------+
| col1        | col2         | col3           |
+-------------+--------------+----------------+
| 101 -/+ 201 | 102          | "103"          |
| 111         | 112 -/+ null | "113" -/+ null |
| 121         | 122          | "123"          |
+-------------+--------------+----------------+
""".trim
    }

    it("returns stringy diff (tableB vs tableA)") {
      tableB.diff(tableA).trim shouldBe """
+-------------+--------------+----------------+
| col1        | col2         | col3           |
+-------------+--------------+----------------+
| 201 -/+ 101 | 102          | "103"          |
| 111         | null -/+ 112 | null -/+ "113" |
| 121         | 122          | "123"          |
+-------------+--------------+----------------+
""".trim
    }

    it("returns stringy diff for column subset") {
      tableB.diff(tableA.select("col1", "col3")).trim shouldBe """
+-------------+----------------+
| col1        | col3           |
+-------------+----------------+
| 201 -/+ 101 | "103"          |
| 111         | null -/+ "113" |
| 121         | "123"          |
+-------------+----------------+
""".trim
    }

    it("uses other table column order (just checking that values are mapped correctly)") {
      tableB.select("col3", "col1", "col2").diff(tableA).trim shouldBe """
+-------------+--------------+----------------+
| col1        | col2         | col3           |
+-------------+--------------+----------------+
| 201 -/+ 101 | 102          | "103"          |
| 111         | null -/+ 112 | null -/+ "113" |
| 121         | 122          | "123"          |
+-------------+--------------+----------------+
""".trim
    }

    it("returns stringy diff for row subset (when comparing longer vs shorter)") {
      tableB.diff(tableA.limit(1)).trim shouldBe """
+-------------+--------------+-----------------+
| col1        | col2         | col3            |
+-------------+--------------+-----------------+
| 201 -/+ 101 | 102          | "103"           |
| 111 -/+ 101 | null -/+ 102 | null -/+ "103"  |
| 121 -/+ 101 | 122 -/+ 102  | "123" -/+ "103" |
+-------------+--------------+-----------------+
""".trim
    }

    it("returns stringy diff for row subset (when comparing shorter vs longer)") {
      tableB.limit(1).diff(tableA).trim shouldBe """
+-------------+------+-------+
| col1        | col2 | col3  |
+-------------+------+-------+
| 201 -/+ 101 | 102  | "103" |
+-------------+------+-------+
""".trim
    }

    it("returns stringy diff when comparing with empty table") {
      tableA.diff(emptyTable).trim shouldBe """
+--------------+--------------+----------------+
| col1         | col2         | col3           |
+--------------+--------------+----------------+
| 101 -/+ null | 102 -/+ null | "103" -/+ null |
| 111 -/+ null | 112 -/+ null | "113" -/+ null |
| 121 -/+ null | 122 -/+ null | "123" -/+ null |
+--------------+--------------+----------------+
""".trim
    }

    it("returns stringy diff when comparing empty table with other table") {
      emptyTable.diff(tableB).trim shouldBe """
+--------------+--------------+----------------+
| col1         | col2         | col3           |
+--------------+--------------+----------------+
| null -/+ 201 | null -/+ 102 | null -/+ "103" |
| null -/+ 111 | null         | null           |
| null -/+ 121 | null -/+ 122 | null -/+ "123" |
+--------------+--------------+----------------+
""".trim
    }

    it("returns same table when comparing identical tables") {
      tableA.diff(tableA).trim shouldBe """
+------+------+-------+
| col1 | col2 | col3  |
+------+------+-------+
| 101  | 102  | "103" |
| 111  | 112  | "113" |
| 121  | 122  | "123" |
+------+------+-------+
""".trim
    }

    it("returns *Both tables are empty* when comparing empty tables") {
      emptyTable.diff(emptyTable) shouldBe "*Both tables are empty*"
    }

    it("throws exception for missing columns") {
      intercept[AnalysisException] {
        tableA.diff(totallyDifferentTable)
      }.getMessage shouldBe "cannot resolve 'bar' given input columns: [col1, col2, col3];"
    }
  }
}
