/*
 * Main.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package scaladatabase

import campaigns._
import scala.collection._

object Main {

    var aCodCampaign = "";
    var aTimeKey = "";
    var aDatabase = "";
    var aDbType = "";
    var aSegment = false;

    val maxConnections = 8
    var countRecs = 0

    val startTime = System.currentTimeMillis()

    def elapsed():Unit = {

        // Get elapsed time in milliseconds
        val elapsedTimeMillis = System.currentTimeMillis()-startTime;
        // Get elapsed time in seconds
        val elapsedTimeSec = elapsedTimeMillis/1000F;
        println("elapsedTimeSec="+elapsedTimeSec)

    }
    /**
     * @param args the command line arguments
     */
    def main(args: Array[String]) :Unit = {

        // Get current time
        processArgs(args)

        // Create and start connection pool
        conPool.start
        println("Started Connection Pool...")

        // Campaign Model
        m.start
        // Task Scheduler
        ts.start

        //val expr = " 1 + KPIA_BGT/KPIB_BVA "
        //val pexpr:expressions.Expr = expressions.ExprParser(expr)
        //println(pexpr.getClass + ": " + pexpr)
        //println("Parsing Expression "+expr+" = "+expressions.ExprParser.test(expr))
        //println("Evaluating Expression "+expr+" = "+expressions.Calc.evaluate(expr))
        
        println("Waiting..")
        Thread.sleep(5000000)
        println("End Waiting..")

        println("Total Records: "+countRecs)

    }

    def processArgs(allArgs: Array[String]):Unit =  {

        //    for (i <- 0 to args.length-1) {
        //      if (i==0) aDbType      = args(0)
        //      if (i==1) aDatabase    = args(1)
        //      if (i==2) aCodCampaign = args(2)
        //      if (i==3) aTimeKey     = args(3)
        //    }

        val options = allArgs.filter(arg => arg.contains("+"))
        val args    = allArgs.filter(arg => !arg.contains("+"))


        options.foreach(option => {
                option.toUpperCase match {
                    case "+SEGMENT" => aSegment = true
                }
            })

        var i = 0
        args.foreach(a => {
                if (i==0) aDbType      = a
                if (i==1) aDatabase    = a
                if (i==2) aCodCampaign = a
                if (i==3) aTimeKey     = a
                i = i+1
            })

        println("Db Type  : " + aDbType);
        println("Database : " + aDatabase);
        println("Campaign : " + aCodCampaign);
        println("Time Key : " + aTimeKey);
        println("Segment  : " + aSegment);
    }



}
