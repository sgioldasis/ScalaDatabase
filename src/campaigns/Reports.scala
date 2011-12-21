/*
 * Reports.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package campaigns

import scala.collection.mutable.HashMap

case class ReportTable(codCampaign:String, codReport:Int, tableNum:Int) extends Ordered[ReportTable] {
    
    private def id = codReport*1000000+tableNum

    var mapLines = new HashMap[Int, ReportLine]
    var setLines:Set[Int] = Set()
    var maxLine = 0

    def name = "CM_REPORT_" + codCampaign + "_R" + codReport + "_T" + tableNum

    def compare(that: ReportTable) = (this.id).compare(that.id)
    def equals(that: ReportTable) = (this.id).equals(that.id)

    def line(lineNum:Int) = {
        if (!mapLines.contains(lineNum)) {
          val newLine = new ReportLine(lineNum)
          mapLines += lineNum -> newLine
          setLines += lineNum
          if (lineNum > maxLine) maxLine = lineNum
        }
        mapLines(lineNum)
    }

}

class ReportLine(lineNum:Int) {

    var mapColumns = new HashMap[Int, ReportColumn]
    var setColumns:Set[Int] = Set()
    var maxColumn = 0

    def column(columnNum:Int) = {
        if (!mapColumns.contains(columnNum)) {
          val newColumn = new ReportColumn(columnNum)
          mapColumns += columnNum -> newColumn
          setColumns += columnNum
          if (columnNum > maxColumn) maxColumn = columnNum
        }
        mapColumns(columnNum)
    }

}

class ReportColumn(columnNum:Int) {
    var ref:Ref = null
}
