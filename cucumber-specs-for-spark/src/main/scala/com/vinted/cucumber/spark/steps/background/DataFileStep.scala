package com.vinted.cucumber.spark.steps.background

import com.vinted.cucumber.spark.steps.SpecStep
import cucumber.api.DataTable
import java.io.{FileOutputStream, OutputStream, OutputStreamWriter}
import java.util.zip.GZIPOutputStream
import scala.collection.JavaConversions._

class DataFileStep extends SpecStep {
  Given("""^(gzipped |)file "(.*?)"$""") {
    (compression: String, fileName: String, lines: DataTable) => {
      val output = new FileOutputStream(context.replaceVariables(fileName))
      writeLines(
        maybeCompress(output, compression.trim),
        lines.asList(classOf[String]).toList
      )
    }
  }

  private

  def writeLines(outputStream: OutputStream, lines: List[String]) = {
    val writer = new OutputStreamWriter(outputStream, "UTF-8")

    try {
      lines.foreach { line =>
        writer.write(s"${line.replace("\n", "\\n")}\n")
      }
    } finally {
      writer.close()
    }
  }

  def maybeCompress(outputStream: OutputStream, compression: String): OutputStream = {
    compression match {
      case "" => outputStream
      case "gzipped" => new GZIPOutputStream(outputStream)
      case _ => throw new UnsupportedOperationException(s"Unsupported compression: ${compression}")
    }
  }
}
