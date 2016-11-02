package com.vinted.cucumber.spark.ext

import cucumber.api.DataTable
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import com.vinted.cucumber.spark.spec.TestHive
import scala.collection.JavaConversions.asScalaBuffer

object DataTableExt {
  implicit class HandyDataTable(table: DataTable) {
    val NULLABLE_TYPES = List(
      FloatType,
      DoubleType,
      ByteType,
      IntegerType,
      LongType,
      BooleanType,
      StringType,
      ArrayType(StringType)
    )

    def toSparkType(typeAsString: String) = typeAsString.toLowerCase match {
      case "array[string]" => ArrayType(StringType)
      case typeName => DataType.fromJson(s""""${typeName}"""")
    }

    def columnTypes = table.topCells.toSeq.map(toSparkType)

    def columnNames = table.raw.get(1)

    def columns = columnNames.zip(columnTypes)

    def dataRows = table.cells(2).toList.map(_.toList.zip(columnTypes).map {
      case ("null", dataType) if NULLABLE_TYPES.contains(dataType) => null
      case (value, _: FloatType) => java.lang.Float.parseFloat(value)
      case (value, _: DoubleType) => java.lang.Double.parseDouble(value)
      case (value, _: ByteType) => java.lang.Byte.parseByte(value)
      case (value, _: IntegerType) => java.lang.Integer.parseInt(value)
      case (value, _: LongType) => java.lang.Long.parseLong(value)
      case (value, _: StringType) => value
      case (value, dataType: ArrayType) if dataType.elementType == StringType => value.split("; ").filterNot(_.isEmpty)
      case (value, _: BooleanType) => java.lang.Boolean.parseBoolean(value)
    })

    def toDataFrame = {
      val schema = StructType(
        columns.map {
          case (columnName, columnType) => StructField(
            columnName,
            columnType,
            nullable = true
          )
        }
      )

      val factRDD = TestHive.sparkContext.makeRDD(dataRows.map(Row.fromSeq(_)))

      TestHive.createDataFrame(factRDD, schema)
    }
  }
}
