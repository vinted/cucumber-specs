package com.vinted.cucumber.util

import de.vandermeer.asciitable.v2.V2_AsciiTable
import de.vandermeer.asciitable.v2.render._
import de.vandermeer.asciitable.v2.themes._

object AsciiTable {
  def apply(header: Seq[Any], rows: Seq[Seq[Any]]): String = {
    val columnCount = header.size
    val table = new V2_AsciiTable()

    table.addRule()
    table.addRow(header.map(_.toString): _*)
    table.addRule()

    rows.foreach { row =>
      if (row.size != columnCount) {
        throw new RuntimeException(s"Unexpected row width: ${row.size} vs expected ${columnCount}")
      }

      table.addRow(row.map(_.toString): _*)
    }

    table.addRule()

    render(table).toString()
  }

  private

  def render(table: V2_AsciiTable) = {
    val renderer = new V2_AsciiTableRenderer
    renderer.setTheme(V2_E_TableThemes.PLAIN_7BIT.get())
    renderer.setWidth(new WidthLongestLine())
    renderer.render(table)
  }
}
