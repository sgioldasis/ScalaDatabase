/*
 * CampaignModel.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package campaigns

import java.math.MathContext
import java.io.File
import java.io.FileWriter
import java.text.DecimalFormat;
import java.text.NumberFormat;

import scaladatabase._
//import scalafiles.FileHelper._
import scala.actors.Actor
import scala.actors.Actor._
import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import scala.collection.mutable.Set
import scala.util.parsing.combinator._

import expressions._

// Model Definitions

case class ArgumentNotFoundException(msg:String) extends Exception
case class UnsupportedFunctionException(msg:String) extends Exception


case class Campaign(codCampaign:String,
                    chrCampaign:String,
                    timeKeyFrom:String,
                    timeKeyTo:String,
                    timeFreq:String,
                    timeKeyBoc:String,
                    timeKeyWfrom:String,
                    autoUnitUpd:String,
                    unitMergeFlg:String)

case class RelTime(relPeriod:String,
                   relTimeKeyFrom:String,
                   relTimeKeyTo:String)

case class MisDays(misTimeKey:String,
                   misDays:BigDecimal,
                   misDaysYtd:BigDecimal)

case class SysTime(sysTimeKey:String)

case class Gkpi(codKpi:String,
                bvaMethod:String,
                agrMethod:String,
                hkpiLevel:Int)

case class Kpi(override val codKpi:String,
               kpiPeriod:String,
               kpiFunction:String,
               codItem:String,
               msrType:String,
               msrAttr:String,
               msrUnit:String,
               override val bvaMethod:String,
               override val agrMethod:String) extends Gkpi(codKpi,bvaMethod,agrMethod,1)

case class Hkpi(override val codKpi:String,
                override val hkpiLevel:Int,
                hkpiFunction:String,
                hkpiColumn:String,
                override val bvaMethod:String,
                override val agrMethod:String) extends Gkpi(codKpi,bvaMethod,agrMethod,hkpiLevel)

case class SubKpi(subKpiRole:String,
                  subKpiRef:Ref)

case class Ekpi(codKpi:String,
                kpiExpression:String,
                parsedExpression:Expr)

case class Ckpi(codKpi:String,
                compFunction:String,
                refKpi:Ref,
                baseUnitSql:String,
                groupAgrLevel:String,
                resultLevel:String)

case class Ref(codKpi:String,
               valType:String) {

    def value(u:String):BigDecimal = {
        m.store.retrieve(codKpi, u, valType)
    }
}

case class RefUnit(unitId:String) {
    def unitLevel = unitId.substring(0, 3)
}

case class SubKpiValue(codSubKpi:String,
                       subKpiRole:String,
                       value:BigDecimal)

case class CampaignUnit(unitId:String,
                        codUnit:String,
                        unitLevelChr:String,
                        unitLevel:Int,
                        unitType:String,
                        unitPath:String) extends DimMember(unitId,unitPath) {

    isBase  = unitLevelChr.equals("KAT") || unitLevelChr.equals("UNK")

    isValid = unitId == path.lastPart

}

case class CampaignSegment(segmentId:String,
                           codSegment:String,
                           segmentChr:String,
                           segmentLevel:Int,
                           segmentLevelChr:String,
                           custType:String,
                           custTypeChr:String,
                           segmentPath:String) extends DimMember(segmentId,segmentPath) {

    isBase  = segmentLevelChr.equals("SEG")

    isValid = segmentId == path.lastPart

}

case class NullSegment(segmentId:String,
                       segmentPath:String) extends DimMember(segmentId,segmentPath) {

    isBase  = true

    isValid = true

}

case class mBaseSql(b:String)
case class mKpiLoaded(k:Kpi, valType:String, numRecs:Int)
//case class mKpiBgt(k:Kpi, numRecs:Int)
case class mKpiAggregated(k:Gkpi, valType:String)
case class mKpiEval(k:Hkpi)
//case class mEvalLevel(l:Int)

case class mAggregate(k:Gkpi, valType:String)
case class mEvaluate(k:Hkpi)

object m extends Actor {

    val codCampaign = Main.aCodCampaign
    var store       = new Store
    var dim:Dim     = null
    var dimNull:Dim = null

    var campaign :CampaignReader = null
    var units    :UnitReader     = null
    var segments :SegmentReader  = null
    var systime  :SysTimeReader  = null
    var reltime  :RelTimeReader  = null
    var misdays  :MisDaysReader  = null
    var repdef   :RepdefReader   = null
    var kpis     :KpiReader      = null
    var hkpis    :HkpiReader     = null
    var subkpis  :SubKpiReader   = null
    var ekpis    :EkpiReader     = null
    var ckpis    :CkpiReader     = null

    var kpi:KpiActReader = null
    var budget:BgtReader = null
    var adjust:AdjReader = null
    var bkpi:KpiBgtReader = null

    var basesql:BaseSqlReader = null
    val basesqls = new HashMap[String, BaseSqlReader]

    val gkpis = new HashMap[String, Gkpi]

    // Define Tasks
    val tCampaign   = Task("CAMPAIGN"   ,"DEF1")
    val tUnits      = Task("UNITS"      ,"DEF1")
    val tSegments   = Task("SEGMENTS"   ,"DEF1")
    val tSystime    = Task("SYSTIME"    ,"DEF1")
    val tReltime    = Task("RELTIME"    ,"DEF1")
    val tMisDays    = Task("MISDAYS"    ,"DEF1")
    val tRepdef     = Task("REPDEF"     ,"DEF1")
    val tKpis       = Task("KPIS"       ,"DEF1")
    val tHkpis      = Task("HKPIS"      ,"DEF1")
    val tSubkpis    = Task("SUBKPIS"    ,"DEF1")
    val tEkpis      = Task("EKPIS"      ,"DEF1")
    val tCkpis      = Task("CKPIS"      ,"DEF1")
    val tEval       = Task("EVAL"       ,"EVL1")
    val tBasesqls   = Task("BASESQLS"   ,"DEF2")
    val tBudget     = Task("BUDGET"     ,"LEVEL_1")
    val tAdjust     = Task("ADJUST"     ,"LEVEL_0")

    def newBaseSqlTask(name:String, b:String)= {
        val taskName = name+"."+b
        val t = Task(taskName,"DEF2")
        t.refBaseSql = b
        t
    }

    def endBaseSqlTask(name:String, b:String) = {
        val taskName = name+"."+b
        ts ! mEndTaskName(taskName)
    }

    def newKpiTask(name:String, k:Kpi):Task = {
        val taskName = name+"."+k.codKpi
        val t = Task(taskName,"LEVEL_1")
        t.refKpi = k
        t
    }

    def endKpiTask[T<:Gkpi](groupName:String, k:T) = {
        val taskName = groupName+"."+k.codKpi
        ts ! mEndTaskName(taskName)
    }

    def newHkpiTask(name:String, k:Hkpi):Task = {
        val taskName = name+"."+k.codKpi
        val t = Task(taskName,"LEVEL_"+k.hkpiLevel)
        t.refHkpi = k
        t afterGroups ("LEVEL_1")
        t
    }

    def endHkpiTask(groupName:String, k:Hkpi) = {
        val taskName = groupName+"."+k.codKpi
        ts ! mEndTaskName(taskName)
    }

    def newBaseSqlReader(b:String) = {
        basesql = new BaseSqlReader(b)
        basesqls += b -> basesql
    }

    // Define Task Dependencies
    tReltime    after (tCampaign,tSystime)
    tMisDays    after (tCampaign,tSystime)
    tRepdef     after (tCampaign)
    tSegments   after (tUnits)
    tKpis       after (tCampaign,tReltime,tMisDays,tSegments)
    tBudget     after (tCampaign,tSystime,tKpis)
    tAdjust     after (tCampaign,tSystime)
    tHkpis      after (tCampaign)
    tSubkpis    after (tCampaign)
    tEkpis      after (tCampaign)
    tCkpis      after (tCampaign)
    tBasesqls   afterGroups ("DEF1")
    tEval       afterGroups ("LEVEL_1")

    // Define Activation Commands for each task
    def activateTask(t:Task) = {

        val name = t.name.lastIndexOf('.') match {
            case -1 => t.name
            case x:Int => t.name.substring(0,x)
        }

        val extension = t.name.lastIndexOf('.') match {
            case -1 => ""
            case x:Int => t.name.substring(x+1)
        }

        //        println("taskActivate: Starting task "+name+"."+extension)

        val k = t.refKpi
        val h = t.refHkpi
        val b = t.refBaseSql

        name match {
            case "CAMPAIGN" => campaign = new CampaignReader
            case "UNITS"    => units    = new UnitReader
            case "SEGMENTS" => segments = new SegmentReader
            case "SYSTIME"  => systime  = new SysTimeReader
            case "RELTIME"  => reltime  = new RelTimeReader
            case "MISDAYS"  => misdays  = new MisDaysReader
            case "REPDEF"   => repdef   = new RepdefReader
            case "KPIS"     => kpis     = new KpiReader
            case "BUDGET"   => budget   = new BgtReader
            case "ADJUST"   => adjust   = new AdjReader
            case "HKPIS"    => hkpis    = new HkpiReader
            case "SUBKPIS"  => subkpis  = new SubKpiReader
            case "EKPIS"    => ekpis    = new EkpiReader
            case "CKPIS"    => ckpis    = new CkpiReader
            case "LOAD_ACT" => kpi      = new KpiActReader(k)
            case "LOAD_BGT" => bkpi     = new KpiBgtReader(k)
            case "BASE_SQL" => newBaseSqlReader(b)
            case "AGGR_ACT" => evalActor(k.codKpi) ! mAggregate(k, "ACT")
            case "AGGR_BGT" => evalActor(k.codKpi) ! mAggregate(k, "BGT")
            case "EVAL"     => self ! "EVAL"
            case "BASESQLS" => self ! "BASESQLS"
            case "EVAL_KPI" => evalActor(h.codKpi) ! mEvaluate(h)
        }
    }

    // Define Command to execute when all tasks are done
    //    override def tasksAllDone = {
    //        self!"END"
    //    }

    def submitTasks(tasks:Task*) = {
        for (t <- tasks) ts ! mSubmitTask(t)
    }



    // Submit Tasks to Scheduler
    submitTasks(tCampaign,tUnits,tSegments,tSystime)
    submitTasks(tReltime, tMisDays, tRepdef, tKpis,tBudget,tAdjust,tHkpis,tSubkpis,tEkpis,tCkpis)

    val mapEvalActors = new HashMap[String, KpiEvaluator]

    def evalActor(codKpi:String) = {
        if (!mapEvalActors.contains(codKpi))
        {
            val a = new KpiEvaluator
            a.start
            mapEvalActors += codKpi -> a
        }
        mapEvalActors(codKpi)
    }

    def outFiles = {
        for (rt <- repdef.listReportTables) {
            val fname = rt.name+".csv"
            val file = new File(fname)
            var fw = new FileWriter(file)

            val dkeys = m.dim.listAll.sort(_<_)
            val tkey = m.systime.obj.sysTimeKey
            var line = ""
            val d = ";"
            val eol = "\n";
            println("Writting output file "+fname)
            for (dkey <- dkeys) {
                val rtLines = rt.setLines.toList.sort(_<_)
                val dkeyOut = dkey.replace(".", d)
                for (lineNum <- rtLines) {
                    line = codCampaign+d+tkey+d+dkeyOut+d+rt.codReport+d+rt.tableNum+d+lineNum
                    val rl = rt.line(lineNum)
                    for (columnNum <- 1 to rl.maxColumn) {
                        val rcRef = rl.column(columnNum).ref
                        val rcVal = if (rcRef!=null) rcRef.value(dkey) else null
                        val rcOut = if (rcVal!=null) rcVal.toString else ""
                        line = line + d + rcOut
                    }
                    line = line + eol
                    //                println("Writting output line "+line)
                    fw.write(line)
                }
            }
            fw.close
        }
    }

    def act() {
        loop {
            react {
                case mActivateTask(t) => {
                        Console.println("CampaignModel!mActivateTask: "+t)
                        activateTask(t)
                    }
                case "CAMPAIGN" => {
                        Console.println("CampaignModel!CAMPAIGN: ["+campaign.obj+"]")
                        ts ! mEndTaskName("CAMPAIGN")
                    }
                case "UNITS" => {
                        units.map.foreach(x=>
                            Console.println("CampaignModel!UNITS: ["+x+"]")
                        )
                        dim = units.d
                        ts ! mEndTaskName("UNITS")
                    }
                case "SEGMENTS" => {
                        if (Main.aSegment) {
                            segments.map.foreach(x=>
                                Console.println("CampaignModel!SEGMENTS: ["+x+"]")
                            )

                            // Create a hybrid dimension
                            val dNull=Dim("NULL")
                            dNull add new NullSegment("NULL" , "\\NULL")
                            dimNull = dim merge dNull
                            Console.println("CampaignModel!SEGMENTS: dimNull.listBas="+dimNull.listBas)
                            Console.println("CampaignModel!SEGMENTS: dimNull.listAgr="+dimNull.listAgr)

                            dim = dim merge segments.d

                        }
                        ts ! mEndTaskName("SEGMENTS")
                    }
                case "SYSTIME" => {
                        Console.println("CampaignModel!SYSTIME: ["+systime.obj+"]")
                        ts ! mEndTaskName("SYSTIME")
                    }
                case "RELTIME" => {
                        Periods.calcRelPeriods
                        reltime.map.foreach(x=>
                            Console.println("CampaignModel!RELTIME: ["+x+"]")
                        )
                        ts ! mEndTaskName("RELTIME")
                    }
                case "MISDAYS" => {
                        misdays.map.foreach(x=>
                            Console.println("CampaignModel!MISDAYS: ["+x+"]")
                        )
                        ts ! mEndTaskName("MISDAYS")
                    }
                case "REPDEF" => {
                        Console.println("CampaignModel!REPDEF: ["+repdef.listReportTables+"]")
                        ts ! mEndTaskName("REPDEF")
                    }
                case "ADJUST" => {
                        Console.println("CampaignModel!ADJUST: ["+adjust.numRecs+"]")
                        ts ! mEndTaskName("ADJUST")
                    }
                case "KPIS" => {
                        kpis.set.foreach(k=> {
                                Console.println("CampaignModel!KPIS: ["+k+"]")
                                val valType = if (k.msrType.trim=="BGT") "BGT" else "ACT"
                                val tLoadActTask = newKpiTask("LOAD_"+valType,k)
                                tLoadActTask after (tCampaign,tReltime,tUnits,tAdjust)
                                val tAggrActTask = newKpiTask("AGGR_"+valType,k)
                                tAggrActTask after (tLoadActTask)
                                submitTasks(tLoadActTask,tAggrActTask)
                            }
                        )

                        ts ! mSubmitTask(tEval)
                        ts ! mEndTaskName("KPIS")
                    }
                case "BUDGET" => {
                        Console.println("CampaignModel!BUDGET: ["+budget.numRecs+"]")
                        budget.set.foreach(b=> {
                                if (kpis.map.contains(b)) {
                                    val k=kpis.map(b)
                                    Console.println("CampaignModel!BUDGET: Kpi["+b+"] - Starting BGT Aggregation" )
                                    val tAggrBgtTask = newKpiTask("AGGR_BGT",k)
                                    ts ! mSubmitTask(tAggrBgtTask)
                                }
                            }
                        )
                        ts ! mEndTaskName("BUDGET")
                    }
                case mKpiLoaded(k, valType, numRecs) => {
                        Console.println("CampaignModel!mKpiLoaded: ["+k.codKpi+"."+valType+","+numRecs+"]")
                        endKpiTask("LOAD_"+valType,k)
                    }
                case mKpiAggregated(k, valType) => {
                        Console.println("CampaignModel!mKpiAggregated: "+valType+"["+k.codKpi+"]")
                        endKpiTask("AGGR_"+valType,k)
                    }
                case "HKPIS" => {
                        hkpis.set.foreach(k=> {
                                Console.println("CampaignModel!HKPIS: ["+k+"]")
                            }
                        )
                        ts ! mEndTaskName("HKPIS")
                    }
                case "SUBKPIS" => {
                        subkpis.mapList.foreach(k=> {
                                Console.println("CampaignModel!SUBKPIS: ["+k+"]")
                            }
                        )
                        ts ! mEndTaskName("SUBKPIS")
                    }
                case "EKPIS" => {
                        ekpis.set.foreach(k=> {
                                Console.println("CampaignModel!EKPIS: ["+k+"]")
                            }
                        )
                        ts ! mEndTaskName("EKPIS")
                    }
                case "CKPIS" => {
                        ckpis.set.foreach(k=> {
                                Console.println("CampaignModel!CKPIS: ["+k+"]")
                            }
                        )
                        ckpis.setBaseSql.foreach(b => {
                                Console.println("CampaignModel!CKPIS: BASESQL["+b+"]")
                                val tTaskBaseSql = newBaseSqlTask("BASE_SQL",b)
                                ts ! mSubmitTask(tTaskBaseSql)
                            }
                        )
                        ts ! mSubmitTask(tBasesqls)
                        ts ! mEndTaskName("CKPIS")
                    }
                case mBaseSql(b) => {
                        Console.println("CampaignModel!mBaseSql: ["+b+"]"+basesqls(b).mapAgr)
                        endBaseSqlTask("BASE_SQL",b)
                    }
                case "BASESQLS" => {
                        Console.println("CampaignModel!BASESQLS")
                        ts ! mEndTaskName("BASESQLS")
                    }
                case "EVAL" => {
                        Console.println("CampaignModel!EVAL:")
                        var prevLevel = 1
                        for (curLevel <- 2 to hkpis.maxLevel) {
                            val setCurLevelHkpis = hkpis.set.filter(h => h.hkpiLevel == curLevel)
                            setCurLevelHkpis.foreach(k=> {
                                    val tEvalHkpiTask = newHkpiTask("EVAL_KPI",k)
                                    tEvalHkpiTask afterGroups ("LEVEL_"+prevLevel)
                                    ts ! mSubmitTask(tEvalHkpiTask)
                                })
                            if (!setCurLevelHkpis.isEmpty) prevLevel = curLevel
                        }
                        ts ! mEndTaskName("EVAL")
                    }
                case mKpiEval(k) => {
                        Console.println("CampaignModel!mKpiEval: ["+k.codKpi+"]")
                        endHkpiTask("EVAL_KPI",k)
                    }
                case "END" => {
                        //                        store.display
                        outFiles
                        Console.println("CampaignModel!END.......")
                        Main.elapsed
                        System.exit(0)
                    }
            }
        }
    }
}


class Reader {

    var query = ""

    def init = {}
    def read = {
        conPool ! msg("SELECT", query  , new rsSelectProcessor(this))
    }
    def rec(rs:java.sql.ResultSet) = {}
    def end = {}
}

case class CampaignReader extends Reader {

    var chrCampaign:String  = _
    var timeKeyFrom:String  = _
    var timeKeyTo:String  = _
    var timeFreq:String  = _
    var timeKeyBoc:String  = _
    var timeKeyWfrom:String  = _
    var autoUnitUpd:String  = _
    var unitMergeFlg:String  = _

    var obj:Campaign        = null

    query = "SELECT * FROM CM_CAMPAIGN_IQ WHERE COD_CAMPAIGN='"+ m.codCampaign + "'"

    override def rec(rs:java.sql.ResultSet) = {
        chrCampaign = rs.getString("CHR_CAMPAIGN")
        timeKeyFrom = rs.getString("TIME_KEY_FROM");
        timeKeyTo = rs.getString("TIME_KEY_TO");
        timeFreq = rs.getString("TIME_FREQ");
        timeKeyBoc = rs.getString("TIME_KEY_BOC");
        timeKeyWfrom = rs.getString("TIME_KEY_WFROM");
        autoUnitUpd = rs.getString("AUTO_UNIT_UPD");
        unitMergeFlg = rs.getString("UNIT_MERGE_FLG");
    }

    override def end = {
        obj = new Campaign( m.codCampaign, chrCampaign, timeKeyFrom,
                           timeKeyTo, timeFreq, timeKeyBoc,
                           timeKeyWfrom, autoUnitUpd, unitMergeFlg)
        m ! "CAMPAIGN"
    }

    read
}

case class RelTimeReader extends Reader {

    var relPeriod:String  = _
    var relTimeKeyFrom:String  = _
    var relTimeKeyTo:String  = _

    var obj:RelTime        = null

    var set = new HashSet[RelTime]
    val map = new HashMap[String, RelTime]

    def create(relPeriod:String, relTimeKeyFrom:String, relTimeKeyTo:String ): Unit = {
        val rt = new RelTime( relPeriod, relTimeKeyFrom, relTimeKeyTo)
        set += rt
        map += rt.relPeriod -> rt
    }

    query = "SELECT * FROM CM_RELTIME_IQ WHERE TIME_KEY_CUR='"+ m.systime.obj.sysTimeKey + "'"

    override def rec(rs:java.sql.ResultSet) = {
        relPeriod = rs.getString("REL_PERIOD")
        relTimeKeyFrom = rs.getString("REL_TIME_KEY_FROM");
        relTimeKeyTo = rs.getString("REL_TIME_KEY_TO");
        create(relPeriod, relTimeKeyFrom, relTimeKeyTo)
    }

    override def end = {
        m ! "RELTIME"
    }

    read
}

case class MisDaysReader extends Reader {

    var misTimeKey:String  = _
    var misDays:double  = _
    var misDaysYtd:double  = _

    var obj:MisDays        = null

    var set = new HashSet[MisDays]
    val map = new HashMap[String, MisDays]

    def create(): Unit = {
        obj = new MisDays( misTimeKey, misDays, misDaysYtd )
        set += obj
        map += misTimeKey -> obj
    }

    query = "SELECT * FROM MS_DAYS_HIST_IQ WHERE TIME_KEY='"+ m.systime.obj.sysTimeKey + "'"

    override def rec(rs:java.sql.ResultSet) = {
        misTimeKey = rs.getString("TIME_KEY")

        val DEFAULT_DECIMAL_FORMAT = new DecimalFormat("#.00");

        misDays = rs.getDouble("PERIOD_DAYS");
        val sMisDays = DEFAULT_DECIMAL_FORMAT.format(misDays);
        val nsMisDays = sMisDays.replace(',', '.');
        val bdMisDays = BigDecimal(nsMisDays);

        misDaysYtd = rs.getDouble("PERIOD_DAYS_YTD");
        val sMisDaysYtd = DEFAULT_DECIMAL_FORMAT.format(misDaysYtd);
        val nsMisDaysYtd = sMisDaysYtd.replace(',', '.');
        val bdMisDaysYtd = BigDecimal(nsMisDaysYtd);

        create
    }

    override def end = {
        m ! "MISDAYS"
    }

    read
}

case class SysTimeReader extends Reader {

    var sysTimeKey:String  = _

    var obj:SysTime        = null

    def create(sysTimeKey:String ): Unit = {
        obj = new SysTime(sysTimeKey)
    }

    query = "SELECT * FROM SPA_TIME_IQ WHERE TIME_KEY IN"+
    "(SELECT TIME_KEY_CUR FROM CM_FULL_SYSTIME_IQ WHERE TIME_FREQ IN"+
    "(SELECT TIME_FREQ FROM CM_CAMPAIGN_IQ WHERE COD_CAMPAIGN='"+ m.codCampaign + "'))"

    override def rec(rs:java.sql.ResultSet) = {
        sysTimeKey = rs.getString("TIME_KEY")
    }

    override def end = {
        if(Main.aTimeKey.equals(""))
        create(sysTimeKey)
        else
        create(Main.aTimeKey)

        m ! "SYSTIME"
    }

    read
}

case class KpiReader extends Reader {

    var codKpi:String   = _
    var kpiPeriod:String   = _
    var kpiFunction:String   = _
    var codItem:String   = _
    var msrType:String   = _
    var msrAttr:String   = _
    var msrUnit:String   = _
    var bvaMethod:String   = _
    var agrMethod:String   = _

    var obj:Kpi = null
    var set = new HashSet[Kpi]
    val map = new HashMap[String, Kpi]

    query = "SELECT * FROM CM_KPI_IQ WHERE COD_KPI IN (SELECT COD_KPI FROM CM_CAMPAIGN_KPI_IQ WHERE COD_CAMPAIGN='"+ m.codCampaign + "')"

    override def rec(rs:java.sql.ResultSet) = {
        codKpi = rs.getString("COD_KPI").trim()
        kpiPeriod = rs.getString("KPI_PERIOD").trim()
        kpiFunction = rs.getString("KPI_FUNCTION").trim()
        codItem = rs.getString("COD_ITEM")
        msrType = rs.getString("MSR_TYPE")
        msrAttr = rs.getString("MSR_ATTR")
        msrUnit = rs.getString("MSR_UNIT")
        bvaMethod = rs.getString("BVA_METHOD").trim()
        agrMethod = rs.getString("AGR_METHOD").trim()

        obj = new Kpi( codKpi, kpiPeriod, kpiFunction,
                      codItem, msrType, msrAttr, msrUnit,
                      bvaMethod, agrMethod )

        set += obj
        map += codKpi -> obj
        
        val gobj:Gkpi = obj
        m.gkpis += codKpi -> gobj
        
        //        Console.println("KpiReader.rec: Processed record: ["+codCampaign+", "+codKpi+"]")
    }

    override def end = {
        m ! "KPIS"
    }

    read
}

case class RepdefReader extends Reader {

    var codReport:Int    = _
    var tableNum:Int    = _
    var lineNum:Int    = _
    var columnNum:Int    = _
    var codKpi:String   = _
    var valType:String   = _

    //    var obj:Kpi = null
    var setReports = new HashSet[Int]
    var listReports:List[Int] = List()
    var setTables = new HashSet[Int]
    var listTables:List[Int] = List()
    var setReportTables = new HashSet[ReportTable]
    var listReportTables:List[ReportTable] = List()
    val mapReportTables = new HashMap[(Int,Int), ReportTable]

    query = "SELECT * FROM CM_REPDEF_COLUMNS_IQ WHERE COD_CAMPAIGN='" + m.codCampaign + "'" + " ORDER BY cod_report, table_num, line_num, column_num;"

    override def rec(rs:java.sql.ResultSet) = {
        codReport = rs.getInt("COD_REPORT")
        tableNum = rs.getInt("TABLE_NUM")
        lineNum = rs.getInt("LINE_NUM")
        columnNum = rs.getInt("COLUMN_NUM")
        codKpi = rs.getString("COD_KPI").trim()
        valType = rs.getString("KPI_VALUE").trim()

        val columnRef = new Ref(codKpi, valType)

        var reportTable:ReportTable = null
        val reportTableRef = (codReport,tableNum)
        if (mapReportTables.contains(reportTableRef))
        reportTable = mapReportTables(reportTableRef)
        else {
            reportTable = new ReportTable(m.codCampaign, codReport, tableNum)
            mapReportTables += reportTableRef -> reportTable
        }

        reportTable.line(lineNum).column(columnNum).ref = columnRef

        setReports += codReport
        setTables  += tableNum
        setReportTables += reportTable



        //        obj = new Kpi( codKpi, kpiPeriod, kpiFunction,
        //                      codItem, msrType, msrAttr, msrUnit,
        //                      bvaMethod, agrMethod )
        //
        //        set += obj
        //        map += codKpi -> obj
        //
        //        val gobj:Gkpi = obj
        //        m.gkpis += codKpi -> gobj

        //        Console.println("KpiReader.rec: Processed record: ["+codCampaign+", "+codKpi+"]")
    }

    override def end = {
        listReports = setReports.toList.sort(_<_)
        listTables  = setTables.toList.sort(_<_)
        listReportTables  = setReportTables.toList.sort(_<_)

        m ! "REPDEF"
    }

    read
}

case class HkpiReader extends Reader {

    var codKpi:String   = _
    var hkpiLevel:Int    = _
    var hkpiFunction:String   = _
    var hkpiColumn:String   = _
    var bvaMethod:String   = _
    var agrMethod:String   = _
    var maxLevel:Int    = 1

    var obj:Hkpi = null
    var set = new HashSet[Hkpi]
    val map = new HashMap[String, Hkpi]

    query = "SELECT * FROM CM_KPI_IQ WHERE COD_KPI IN (" + "SELECT COD_KPI FROM CM_CAMPAIGN_HKPI_IQ WHERE COD_CAMPAIGN='" + m.codCampaign + "'" + ")" + " UNION " + "SELECT * FROM CM_KPI_IQ WHERE COD_KPI IN (" + "SELECT COD_KPI FROM CM_CAMPAIGN_EKPI_IQ WHERE COD_CAMPAIGN='" + m.codCampaign + "'" + ")" + " UNION " + "SELECT * FROM CM_KPI_IQ WHERE COD_KPI IN (" + "SELECT COD_KPI FROM CM_CAMPAIGN_CKPI_IQ WHERE COD_CAMPAIGN='" + m.codCampaign + "'" + ")"

    override def rec(rs:java.sql.ResultSet) = {
        codKpi = rs.getString("COD_KPI").trim()
        hkpiLevel = rs.getInt("HKPI_LEVEL")
        hkpiFunction = rs.getString("HKPI_FUNCTION").trim()
        hkpiColumn = rs.getString("HKPI_COLUMN")
        bvaMethod = rs.getString("BVA_METHOD")
        agrMethod = rs.getString("AGR_METHOD")

        if (hkpiLevel > maxLevel) maxLevel = hkpiLevel

        obj = new Hkpi( codKpi, hkpiLevel, hkpiFunction,
                       hkpiColumn, bvaMethod, agrMethod )

        set += obj
        map += codKpi -> obj
        //        Console.println("KpiReader.rec: Processed record: ["+codCampaign+", "+codKpi+"]")

        val gobj:Gkpi = obj
        m.gkpis += codKpi -> gobj

    }

    override def end = {
        m ! "HKPIS"
    }

    read
}

case class SubKpiReader extends Reader {

    var codKpi:String   = _
    var codSubKpi:String   = _
    var subKpiRole:String   = _
    var subKpiColumn:String   = _

    var obj:SubKpi = null
    var set = new HashSet[SubKpi]
    val map = new HashMap[String, SubKpi]
    val mapList = new HashMap[String, List[SubKpi]]

    query = "SELECT * FROM CM_CAMPAIGN_HKPI_IQ WHERE COD_CAMPAIGN='" + m.codCampaign + "' ORDER BY COD_KPI, SUB_KPI_ROLE"

    override def rec(rs:java.sql.ResultSet) = {
        codKpi = rs.getString("COD_KPI").trim();
        codSubKpi = rs.getString("COD_SUB_KPI").trim();
        subKpiRole = rs.getString("SUB_KPI_ROLE");
        subKpiColumn = rs.getString("SUB_KPI_COLUMN");

        if (subKpiRole == null) subKpiRole = "TERM"
        else if (subKpiRole.isEmpty) subKpiRole = "TERM"
        else subKpiRole = subKpiRole.trim()

        if (subKpiColumn == null) subKpiColumn = "ACT"
        else if (subKpiColumn.isEmpty) subKpiColumn = "ACT"
        else subKpiColumn = subKpiColumn.trim()

        obj = new SubKpi( subKpiRole, Ref(codSubKpi, subKpiColumn) )

        set += obj
        map += codKpi -> obj

        if (!mapList.contains(codKpi)) {
            mapList += codKpi -> List(obj)
        } else {
            mapList += codKpi -> (obj :: mapList(codKpi))
        }
        //        Console.println("KpiReader.rec: Processed record: ["+codCampaign+", "+codKpi+"]")
    }

    override def end = {
        m ! "SUBKPIS"
    }

    read
}

case class EkpiReader extends Reader {

    var codKpi:String   = _
    var kpiExpression:String   = _

    var obj:Ekpi = null
    var set = new HashSet[Ekpi]
    val map = new HashMap[String, Ekpi]

    query = "SELECT * FROM CM_CAMPAIGN_EKPI_IQ WHERE COD_CAMPAIGN='" + m.codCampaign + "'"

    override def rec(rs:java.sql.ResultSet) = {
        codKpi = rs.getString("COD_KPI").trim();
        kpiExpression = rs.getString("KPI_EXPRESSION").trim();

        obj = new Ekpi( codKpi, kpiExpression, ExprParser(kpiExpression) )

        set += obj
        map += codKpi -> obj

    }

    override def end = {
        m ! "EKPIS"
    }

    read
}


case class CkpiReader extends Reader {

    var codKpi:String   = _
    var compFunction:String   = _
    var codRefKpi:String   = _
    var refKpiColumn:String   = _
    var baseUnitSql:String   = _
    var groupAgrLevel:String   = _
    var resultLevel:String   = _

    var obj:Ckpi = null
    var set = new HashSet[Ckpi]
    var setBaseSql = new HashSet[String]
    val map = new HashMap[String, List[Ckpi]]

    query = "SELECT * FROM CM_CAMPAIGN_CKPI_IQ WHERE COD_CAMPAIGN='" + m.codCampaign + "'"

    override def rec(rs:java.sql.ResultSet) = {
        codKpi = rs.getString("COD_KPI").trim();
        compFunction = rs.getString("COMP_FUNCTION").trim();
        codRefKpi = rs.getString("COD_REF_KPI").trim();
        refKpiColumn = rs.getString("REF_KPI_COLUMN").trim();
        baseUnitSql = rs.getString("BASE_UNIT_SQL").trim();
        groupAgrLevel = rs.getString("GROUP_AGR_LEVEL").trim();
        resultLevel = rs.getString("RESULT_LEVEL").trim();

        obj = new Ckpi( codKpi, compFunction, Ref(codRefKpi, refKpiColumn), baseUnitSql, groupAgrLevel, resultLevel )

        set += obj
        
        if (!map.contains(codKpi)) map += codKpi -> List(obj)
        else {
            val l=obj::map(codKpi)
            map += codKpi -> l }

        setBaseSql += baseUnitSql

    }

    override def end = {
        m ! "CKPIS"
    }

    read
}



case class UnitReader extends Reader {

    var unitId       : String = _
    var codUnit      : String = _
    var unitLevelChr : String = _
    var unitLevel    : Int    = _
    var unitType     : String = _
    var unitPath     : String = _

    var obj:CampaignUnit = null

    var set = new HashSet[CampaignUnit]
    val map = new HashMap[String, CampaignUnit]()

    val d=Dim("UNIT")

    query = "SELECT * FROM CM_UNIT_CAMPAIGN_IQ WHERE COD_CAMPAIGN='" + m.codCampaign + "' ORDER BY UNIT_ID"

    override def rec(rs:java.sql.ResultSet) = {
        unitId = rs.getString("UNIT_ID")
        codUnit = rs.getString("COD_UNIT")
        unitLevelChr = rs.getString("UNIT_LEVEL_CHR")
        unitLevel = rs.getInt("UNIT_LEVEL")
        unitType = rs.getString("UNIT_TYPE")
        unitPath = rs.getString("UNIT_PATH")

        obj = new CampaignUnit(unitId ,codUnit,unitLevelChr,
                               unitLevel,unitType,unitPath)

        d add obj

        if (obj.isValid) {
            set += obj
            map += unitId -> obj
        }

    }

    override def end = {
        m ! "UNITS"
    }

    read
}

case class SegmentReader extends Reader {

    var segmentId       :String = _
    var codSegment      :String = _
    var segmentChr      :String = _
    var segmentLevel    :Int    = _
    var segmentLevelChr :String = _
    var custType        :String = _
    var custTypeChr     :String = _
    var segmentPath     :String = _

    var obj:CampaignSegment = null

    var set = new HashSet[CampaignSegment]
    val map = new HashMap[String, CampaignSegment]()

    val d=Dim("SEGMENT")

    query = "SELECT * FROM CM_SEGMENTS_IQ"

    override def rec(rs:java.sql.ResultSet) = {
        segmentId = rs.getString("SEGMENT_ID")
        codSegment = rs.getString("COD_SEGMENT")
        segmentChr = rs.getString("CHR_SEGMENT")
        segmentLevel = rs.getInt("COD_LEVEL_SEGMENT")
        segmentLevelChr = rs.getString("CHR_LEVEL_SEGMENT")
        custType = rs.getString("COD_CUST_TYPE")
        custTypeChr = rs.getString("CHR_CUST_TYPE")
        segmentPath = rs.getString("SEGMENT_PATH")

        obj = new CampaignSegment(segmentId ,codSegment,segmentChr,
                                  segmentLevel,segmentLevelChr,custType,
                                  custTypeChr, segmentPath)

        d add obj

        if (obj.isValid) {
            set += obj
            map += segmentId -> obj
        }

    }

    override def end = {
        m ! "SEGMENTS"
    }

    read
}

case class BaseSqlReader(baseSql:String) extends Reader {

    var unitId       : String = _
    var codUnit      : String = _
    var unitLevelChr : String = _
    var unitLevel    : Int    = _
    var unitType     : String = _
    var unitPath     : String = _

    var obj:CampaignUnit = null

    var set = new HashSet[CampaignUnit]

    val map = new HashMap[String, CampaignUnit]()
    val mapAgr = new HashMap[String, HashMap[String, Set[String]]]()

    query = "SELECT * FROM CM_UNIT_CAMPAIGN_IQ WHERE COD_CAMPAIGN='" + m.codCampaign + "' AND " + baseSql

    override def rec(rs:java.sql.ResultSet) = {
        unitId = rs.getString("UNIT_ID")
        codUnit = rs.getString("COD_UNIT")
        unitLevelChr = rs.getString("UNIT_LEVEL_CHR")
        unitLevel = rs.getInt("UNIT_LEVEL")
        unitType = rs.getString("UNIT_TYPE")
        unitPath = rs.getString("UNIT_PATH")

        obj = new CampaignUnit(unitId ,codUnit,unitLevelChr,
                               unitLevel,unitType,unitPath)

        if (obj.isValid) {
            set += obj
            map += unitId -> obj

            obj.pathAgrParts foreach {a =>
                {
                    val groupAgrLevel = a.substring(0, 3)
                    if (!mapAgr.contains(groupAgrLevel))
                    mapAgr += groupAgrLevel -> new HashMap[String, Set[String]]()

                    if (mapAgr(groupAgrLevel).contains(a))
                    mapAgr(groupAgrLevel)(a) += unitId
                    else
                    mapAgr(groupAgrLevel) += a -> Set(unitId)
                }
            }
        }

    }

    override def end = {
        m ! mBaseSql(baseSql)
    }

    read
}


case class BgtReader extends Reader {

    var unitId       : String = _
    var codKpi       : String = _
    var bgtValue     : double = _

    var numRecs      : Int    = _

    var set = new HashSet[String]

    query = "SELECT * FROM CM_UNIT_BUDGET_CAMPAIGN_KPI_IQ" +
    " WHERE COD_CAMPAIGN='" + m.codCampaign + "'" +
    " AND '" + m.systime.obj.sysTimeKey + "' BETWEEN TIME_KEY_FROM AND TIME_KEY_TO" +
    " AND UNIT_ID IN (SELECT UNIT_ID FROM CM_UNIT_CAMPAIGN_IQ" +
    " WHERE COD_CAMPAIGN='" + m.codCampaign + "')" +
    " ORDER BY COD_KPI"

    override def rec(rs:java.sql.ResultSet) = {
        unitId = rs.getString("UNIT_ID").trim()
        codKpi = rs.getString("COD_KPI").trim()
        bgtValue = rs.getDouble("BGT_KPI_VAL");

        val DEFAULT_DECIMAL_FORMAT = new DecimalFormat("#.00");
        val sBgtValue = DEFAULT_DECIMAL_FORMAT.format(bgtValue);
        val nsBgtValue = sBgtValue.replace(',', '.');
        val bdBgtValue = BigDecimal(nsBgtValue);

        m.store.insert(codKpi, unitId, "BGT", bdBgtValue)

        set += codKpi
        numRecs += 1
    }

    override def end = {
        m ! "BUDGET"
    }

    read
}


case class KpiActReader(k: Kpi) extends Reader {

    var unitPath     : String = _
    var actValue     : double = _

    var numRecs      : Int    = _

    var unitId       : String = _
    var codUnit      : String = _
    var unitLevelChr : String = _
    var unitLevel    : Int    = _
    var unitType     : String = _

    var segmentId    : String = _

    var obj:CampaignUnit = null

    var set = new HashSet[CampaignUnit]
    var setBas = new HashSet[CampaignUnit]
    var setAgr = new HashSet[CampaignUnit]

    val map = new HashMap[String, CampaignUnit]()
    val mapAgr = new HashMap[String, Set[String]]()

    val to   = m.reltime.map(k.kpiPeriod).relTimeKeyTo
    val from = m.reltime.map(k.kpiPeriod).relTimeKeyFrom

    val que_part1 =
    "SELECT U.UNIT_PATH, F.SEGMENT_ID, "

    val fDIF = k.kpiFunction.trim.equalsIgnoreCase("DIF")
    val fSUM = k.kpiFunction.trim.equalsIgnoreCase("SUM")
    val fAVG = k.kpiFunction.trim.equalsIgnoreCase("AVG")

    val que_part2 = if (fDIF)
    "SUM( CASE WHEN TIME_KEY='" + to   + "' THEN ACT_MSR_VAL ELSE 0 END )" +
    " - " +
    "SUM( CASE WHEN TIME_KEY='" + from + "' THEN ACT_MSR_VAL ELSE 0 END )"
    else if (fSUM) " SUM(ACT_MSR_VAL)"
    else if (fAVG) " AVG(ACT_MSR_VAL)"
    else " SUM(ACT_MSR_VAL)"

    val que_part3 =
    " AS ACT_VALUE" +
    " FROM CM_MSR_FACT_IQ AS F, CM_UNIT_CAMPAIGN_IQ AS U" +
    " WHERE U.COD_CAMPAIGN='" + m.codCampaign + "'" +
    " AND F.UNIT_ID=U.UNIT_ID" +
    " AND F.COD_ITEM='" + k.codItem + "'" +
    " AND F.MSR_TYPE='" + k.msrType + "'" +
    " AND F.MSR_ATTR='" + k.msrAttr + "'" +
    " AND F.MSR_UNIT='" + k.msrUnit + "'";

    val que_part4 = if (fDIF)
    " AND F.TIME_KEY IN ('" + from + "','" + to + "')"
    else
    " AND F.TIME_KEY BETWEEN '" + from + "' AND '" + to + "'"

    val que_part5 =
    " GROUP BY U.UNIT_PATH, F.SEGMENT_ID "

    query = que_part1 + que_part2 + que_part3 + que_part4 + que_part5
    //    Console.println("KpiActReader Starting Query: "+ query)

    override def rec(rs:java.sql.ResultSet) = {
        unitPath = rs.getString("UNIT_PATH").trim()
        segmentId = rs.getString("SEGMENT_ID")
        actValue = rs.getDouble("ACT_VALUE");

        segmentId = if (segmentId==null) "" else segmentId
        segmentId = if (segmentId.trim()=="") "NULL" else segmentId

        val DEFAULT_DECIMAL_FORMAT = new DecimalFormat("#.00");
        val sActValue = DEFAULT_DECIMAL_FORMAT.format(actValue);
        val nsActValue = sActValue.replace(',', '.');
        val bdActValue = BigDecimal(nsActValue);

        val upath = Path(unitPath)
        val ukey  = upath.lastPart

        val key   = if (Main.aSegment) ukey+"."+segmentId else ukey

        m.store.insert(k.codKpi, key, "ACT", bdActValue)
        numRecs += 1
    }

    override def end = {
        m ! mKpiLoaded(k, "ACT", numRecs)
    }

    read
}

case class KpiBgtReader(k: Kpi) extends Reader {

    var unitPath     : String = _
    var bgtValue     : double = _

    var numRecs      : Int    = _

    var unitId       : String = _
    var codUnit      : String = _
    var unitLevelChr : String = _
    var unitLevel    : Int    = _
    var unitType     : String = _

    var obj:CampaignUnit = null

    var set = new HashSet[CampaignUnit]
    var setBas = new HashSet[CampaignUnit]
    var setAgr = new HashSet[CampaignUnit]

    val map = new HashMap[String, CampaignUnit]()
    val mapAgr = new HashMap[String, Set[String]]()

    val to   = m.reltime.map(k.kpiPeriod).relTimeKeyTo
    val from = m.reltime.map(k.kpiPeriod).relTimeKeyFrom

    val que_part1 =
    "SELECT U.UNIT_PATH, "

    val fDIF = k.kpiFunction.trim.equalsIgnoreCase("DIF")
    val fSUM = k.kpiFunction.trim.equalsIgnoreCase("SUM")
    val fAVG = k.kpiFunction.trim.equalsIgnoreCase("AVG")

    val que_part2 = if (fDIF)
    "SUM( CASE WHEN TIME_KEY_TO='" + to   + "' THEN BGT_KPI_VAL ELSE 0 END )" +
    " - " +
    "SUM( CASE WHEN TIME_KEY_FROM='" + from + "' THEN BGT_KPI_VAL ELSE 0 END )"
    else if (fSUM) " SUM(BGT_KPI_VAL)"
    else if (fAVG) " AVG(BGT_KPI_VAL)"
    else " SUM(BGT_KPI_VAL)"

    val que_part3 =
    " AS BGT_VALUE" +
    " FROM CM_UNIT_BUDGET_CAMPAIGN_KPI_IQ AS F, CM_UNIT_CAMPAIGN_IQ AS U" +
    " WHERE U.COD_CAMPAIGN='" + m.codCampaign + "'" +
    " AND F.UNIT_ID=U.UNIT_ID" +
    " AND COD_KPI = '" + k.msrAttr.substring(0, 9) + "'"
    " AND F.TIME_KEY_FROM = F.TIME_KEY_TO"

    val que_part4 = if (fDIF)
    " AND F.TIME_KEY_FROM IN ('" + from + "','" + to + "')"
    else
    " AND F.TIME_KEY_FROM BETWEEN '" + from + "' AND '" + to + "'"

    val que_part5 =
    " GROUP BY U.UNIT_PATH "

    query = que_part1 + que_part2 + que_part3 + que_part4 + que_part5
    //    Console.println("KpiActReader Starting Query: "+ query)

    override def rec(rs:java.sql.ResultSet) = {
        unitPath = rs.getString("UNIT_PATH").trim()
        bgtValue = rs.getDouble("BGT_VALUE");

        val DEFAULT_DECIMAL_FORMAT = new DecimalFormat("#.00");
        val sBgtValue = DEFAULT_DECIMAL_FORMAT.format(bgtValue);
        val nsBgtValue = sBgtValue.replace(',', '.');
        val bdBgtValue = BigDecimal(nsBgtValue);

        val upath = Path(unitPath)
        val ukey  = upath.lastPart

        m.store.insert(k.codKpi, ukey, "BGT", bdBgtValue)
        numRecs += 1
    }

    override def end = {
        m ! mKpiLoaded(k, "BGT", numRecs)
    }

    read
}

case class AdjReader extends Reader {

    var unitId       : String = _
    var codKpi       : String = _
    var adjValue     : double = _

    var numRecs      : Int    = _

    var set = new HashSet[String]

    query = "SELECT * FROM CM_FILE_ADJ_KPI_FACT_IQ" +
    " WHERE COD_CAMPAIGN='" + m.codCampaign + "'" +
    " AND TIME_KEY='" + m.systime.obj.sysTimeKey + "'"

    override def rec(rs:java.sql.ResultSet) = {
        unitId = rs.getString("UNIT_ID").trim()
        codKpi = rs.getString("COD_KPI").trim()
        adjValue = rs.getDouble("ACT_KPI_VAL");

        val DEFAULT_DECIMAL_FORMAT = new DecimalFormat("#.00");
        val sAdjValue = DEFAULT_DECIMAL_FORMAT.format(adjValue);
        val nAdjValue = sAdjValue.replace(',', '.');
        val bdAdjValue = BigDecimal(nAdjValue);

        m.store.insert(codKpi, unitId, "ADJ", bdAdjValue)

        set += codKpi
        numRecs += 1
    }

    override def end = {
        m ! "ADJUST"
    }

    read
}


trait KpiAggregator {
    
    private var agrKpi: Gkpi = null
    private var agrValType:String = null
    private var dim:Dim     = null


    private def storeRead(key:String) = {
        m.store.retrieve(agrKpi.codKpi, key, agrValType)
    }

    private def storeWrite(key:String, value:BigDecimal) = {
        m.store.insert(agrKpi.codKpi, key, agrValType, value)
    }

    private def getVals(listKeys:List[String]):List[BigDecimal] = {
        listKeys map storeRead
    }

    private def af_sum(listBvals:List[BigDecimal]):BigDecimal = {
        val termsNotNull = listBvals.filter{i => i != null}
        if (termsNotNull.length > 0) termsNotNull.reduceLeft(_+_)
        else null
    }

    private def af_cnt(listBvals:List[BigDecimal]):BigDecimal = {
        val cnt:BigDecimal = listBvals.length
        cnt
    }

    private def af_avg(listBvals:List[BigDecimal]):BigDecimal = {
        var avg:BigDecimal = null
        val cnt = af_cnt(listBvals)
        val sum = af_sum(listBvals)

        if (cnt != 0) {
            val bd = sum.bigDecimal.divide(cnt.bigDecimal,MathContext.DECIMAL128)
            avg = new BigDecimal(bd)
        } else avg = null
        
        avg
    }

    private def applyAgrFunction(listBvals:List[BigDecimal]):BigDecimal = {
        agrKpi.agrMethod match {
            case "SUM" => af_sum(listBvals)
            case "AVG" => af_avg(listBvals)
            case "CNT" => af_cnt(listBvals)
            case _     => af_sum(listBvals)
        }
    }

    private def aggregateKey(agrKey:String) = {

        var agrVal:BigDecimal = null
        val agrInStore  = storeRead(agrKey)!=null
        val agrIsBudget = agrValType=="BGT"
        val agrNeeded   = if (Main.aSegment) !agrInStore else !(agrIsBudget && agrInStore)

        if (agrNeeded)
        {
            val listBkeys = if (dim.mapAgr.contains(agrKey)) dim.mapAgr(agrKey) else Nil
            val listBvals = getVals(listBkeys)
            agrVal        = applyAgrFunction(listBvals)
            storeWrite(agrKey,agrVal)
            // Normally we should only perform aggregation if
            // store does not already contain a (fixed) value
            // eg. if store contains a budget value for an area we don't want
            //     to overwrite it with a calculated value
            // However, because such cases exist we issue a warning
            if (agrInStore) {
                println("Warning! Overwritten stored value for Aggregation Key ["+agrKey+"]" )
            }

        }

        Console.println("Aggregate "+agrValType+" of Dim["+dim.name+"],Key["+agrKey+"] for KPI["+agrKpi.codKpi+","+agrKpi.agrMethod+"] = "+agrVal)
    }

    def aggregate(pk: Gkpi, pValType:String) = {

        agrKpi = pk
        agrValType = pValType

        //        m.dim.listAgr.foreach(agrKey => aggregateKey(agrKey))

        Console.println("Aggregate was called: pk="+pk+", pValType="+pValType )
        dim=m.dim
        dim.listAgr map aggregateKey

        if (Main.aSegment) {
            dim=m.dimNull
            dim.listAgr map aggregateKey
        }

        sender ! mKpiAggregated(agrKpi, pValType)
        //exit()
    }

}

trait HkpiBasics {

    var k:Hkpi = null
    var u:String = null

    def storeWriteHkpi(value:BigDecimal) = {
        m.store.insert(k.codKpi, u, k.hkpiColumn, value)
    }

    def getArguments(argName:String):List[BigDecimal] = {

        for (SubKpi(subKpiRole , ref) <- m.subkpis.mapList(k.codKpi) if (argName == subKpiRole)) yield ref.value(u)

    }

    def getArgument(argName:String):BigDecimal = {
        val argList = getArguments(argName)

        if (!argList.isEmpty) //!=Nil
        getArguments(argName).head
        else 
        throw new ArgumentNotFoundException("Argument not found:"+argName)

    }

}

trait HkpiProcessor extends HkpiFunctions {

    def processHkpi = {
        m.dim.listAll map processHkpiKey
    }

    def processHkpiKey(dimKey:String) = {

        u = dimKey
        var result:BigDecimal = null

        try {
            result=applyHkpiFunction()
            storeWriteHkpi(result)
            Console.println("HKPI Function "+k.hkpiFunction+" for [HKPI:"+k.codKpi+", DimKey:"+u+"] = "+result)
        } catch {
            case ex: ArgumentNotFoundException    => {Console.println("HKPI Function "+k.hkpiFunction+" for [HKPI:"+k.codKpi+", DimKey:"+u+"] = Exception:"+ex.msg)}
            case ex: UnsupportedFunctionException => {Console.println("HKPI Function "+k.hkpiFunction+" for [HKPI:"+k.codKpi+", DimKey:"+u+"] = Exception:"+ex.msg)}
        }
    }

}


case class cfnDef (inpType:String)


case class DimVal (u:String, v:BigDecimal) extends Ordered[DimVal] {
    def compare(that: DimVal) = (this.v).compare(that.v)
    def equals(that: DimVal) = (this.v).equals(that.v)
}

trait CkpiBasics extends HkpiBasics {

    var gName:String = null
    var gMembers:Set[String] = null
    var ckpi:Ckpi = null

    def getMemberValues:List[BigDecimal] = {
        for (u <- gMembers.toList) yield ckpi.refKpi.value(u)
    }

    def getMemberDimVals = {
        for (u <- gMembers.toList) yield DimVal(u,ckpi.refKpi.value(u))
    }


}


trait CkpiProcessor extends CkpiFunctions {

    def processCkpis = {
        m.ckpis.map(k.codKpi).foreach(c => processCkpi(c))
    }

    def processCkpi(c:Ckpi) = {

        ckpi = c
        val sqlReader = m.basesqls(ckpi.baseUnitSql)
        var groups = new HashMap[String, Set[String]]()
        if (sqlReader.mapAgr.contains(ckpi.groupAgrLevel))
        groups = sqlReader.mapAgr(ckpi.groupAgrLevel)

        //        Console.println("Processing COMPARIZON Function "+ ckpi.compFunction +", baseUnitSql="+ ckpi.baseUnitSql+", groupAgrLevel="+ ckpi.groupAgrLevel+", ckpi="+ k.codKpi+", groups="+ groups)
        //        Console.println("CKPI Function "+ ckpi.compFunction +" for [CKPI:"+k.codKpi+"], baseUnitSql="+ ckpi.baseUnitSql+", groupAgrLevel="+ ckpi.groupAgrLevel+", groups="+ groups)

        groups.foreach(g => {

                gName  = g._1
                gMembers = g._2

                val name = g._1
                val members = g._2
                val sunits = m.units.d.mapSunits(name)

                var refKpi = ckpi.refKpi
                var grpVal:BigDecimal = null
                var mbrVal:List[DimVal] = null

                Console.println("COMPARIZON for [CKPI:"+k.codKpi+", Group:"+name+"], groupAgrLevel="+ ckpi.groupAgrLevel+", baseUnitSql="+ ckpi.baseUnitSql+", members="+ members)

                select_cfn match {
                    case (cfnDef("GRPVAL"),cfn,mfn) => {
                            grpVal = cfn()
                            var resultSet:Set[String] = null
                            // Need to distribute result according to resultLevel
                            if (ckpi.resultLevel.equals("SUNIT")) resultSet = sunits + name
                            else resultSet = members // + name
                            resultSet.foreach(u => {
                                    m.store.insert(k.codKpi, u, k.hkpiColumn, grpVal)
                                })

                            Console.println("CKPI Function-G "+ ckpi.compFunction +" for [CKPI:"+k.codKpi+", Group:"+name+"], grpVal="+ grpVal+ ", resultLevel="+ ckpi.resultLevel + ", resultSet="+ resultSet)

                        }

                    case (cfnDef("MBRVAL"),cfn,mfn) => {
                            mbrVal = mfn()
                            //                            println("mbrVal="+mbrVal)
                            for (DimVal(u, v) <- mbrVal)
                            {
                                m.store.insert(k.codKpi, u, k.hkpiColumn, v)
                            }
                            Console.println("CKPI Function-M "+ ckpi.compFunction +" for [CKPI:"+k.codKpi+", Group:"+name+"], mbrVal"+ mbrVal)
                        }

                    case (cfnDef("UKNOWN"),cfn,mfn) => {
                            var values:Set[(String,BigDecimal)] = Set()
                            members.foreach(u => {
                                    val refKpi = ckpi.refKpi
                                    //                                    Console.println("COMPARIZON Group "+ name + " refKpi: "+ refKpi)
                                    val value:(String,BigDecimal) = (u,ckpi.refKpi.value(u))
                                    values += value
                                })
                            Console.println("Unsupported CKPI Function: "+ ckpi.compFunction +" for [CKPI:"+k.codKpi+", Group:"+name+"]")

                        }
                }
            }
        )
    }
}

case class KpiEvaluator extends Actor with HkpiProcessor with CkpiProcessor with KpiAggregator {

    def evaluate = {
        if (m.budget.set.contains(k.codKpi)) aggregate(k,"BGT")
        if (k.hkpiFunction.equals("COMPARISON")) processCkpis
        else processHkpi
    }

    def act() {
        loop {
            react {
                case mEvaluate(h) => {
                        k = h
                        evaluate
                        m ! mKpiEval(k)
                        exit()
                    }
                case mAggregate(k,valType) => aggregate(k,valType)
            }
        }
    }

}

