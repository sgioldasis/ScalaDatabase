/*
 * Functions_Bva.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package campaigns
import java.math.MathContext;

trait BvaFunctions {

    var bva:BigDecimal = null

    def calcBva(codKpi:String, dimKey:String, valType:String):BigDecimal = {

        val bgt = m.store.retrieve(codKpi, dimKey, "BGT")
        val act = m.store.retrieve(codKpi, dimKey, "ACT")

        
        if (act != null && bgt != null) {

            val gkpi = m.gkpis(codKpi)
            val method = gkpi.bvaMethod

            m.gkpis(codKpi).bvaMethod match {
                case "BVA" => bva = method_BVA(act, bgt)
                case null  => bva = method_BVA(act, bgt)
                case "NBA" => bva = method_NBA(act, bgt)
                case "NPL" => bva = method_NPL(act, bgt)
                case "ZBA" => bva = method_ZBA(act, bgt)
                    //          case "BAS" => method_BAS(act, bgt)
                case x     => bva = method_unsupported(x)
            }
        } else bva = null
        
        bva
    }

    def percentage(a:BigDecimal, b:BigDecimal):BigDecimal = {
        val d = new BigDecimal(a.bigDecimal.divide(b.bigDecimal, MathContext.DECIMAL128))
        d * 100
    }

    def method_unsupported(methodName:String):BigDecimal = {
        println ("Unsupported BvaMethod: ["+methodName+"]")
        null
    }

    def method_NBA(act:BigDecimal, bgt:BigDecimal):BigDecimal = {
        if (bgt <= 0) if (act < 0) 0 else 100
        else          if (act < 0) 0 else percentage(act,bgt)
    }

    def method_BVA(act:BigDecimal, bgt:BigDecimal):BigDecimal = {
        if (bgt <= 0) if (act >= bgt) 100 else 0
        else          if (act < 0)    0   else percentage(act,bgt)
    }

    def method_NPL(act:BigDecimal, bgt:BigDecimal):BigDecimal = {
        if (act <= bgt) 100 else 0
    }

    def method_ZBA(act:BigDecimal, bgt:BigDecimal):BigDecimal = {
        if (bgt <= 0) 0
        else          if (act < 0)    0   else percentage(act,bgt)
    }

}
