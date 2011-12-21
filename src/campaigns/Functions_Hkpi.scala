/*
 * Functions_Hkpi.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package campaigns

import java.math.MathContext;

trait HkpiFunctions extends HkpiBasics with BvaFunctions {

    def applyHkpiFunction():BigDecimal = {

        k.hkpiFunction match {
            case "NULL2ZERO" => fn_null2zero()
            case "SUM" => fn_sum()
            case "SQL_SUM" => fn_sum()
            case "DIV_PERIODS" => fn_div_periods()
            case "REM_MONTHS" => fn_rem_months()
            case "EXPRESSION" => fn_expression()
            case "PERCENTAGE" => fn_percentage()
            case "DIVISION" => fn_division()
            case "CALC_BVA" => fn_calc_bva()
            case "CALC_NBA" => fn_calc_nba()
            case "CALC_NPL" => fn_calc_npl()
            case "DIFF_PERCENT" => fn_diff_percent()
            case "AVG_INT" => fn_avg_int("CURRENT")
            case "AVG_INT_YTD" => fn_avg_int("YTD")
            case "SPREAD" => fn_spread("CURRENT")
            case "SPREAD_YTD" => fn_spread("YTD")
            case "EQRATES_BVA" => fn_eqrates_bva()
            case x => throw new UnsupportedFunctionException("Unsupported Function:"+x)
        }

    }

    def fn_null2zero():BigDecimal = {

        val rf = getArgument("REFERENCE")
        if (rf==null) 0 else rf

    }

    def fn_sum():BigDecimal = {

        val terms = getArguments("TERM")
        val termsNotNull = terms.filter{i => i != null}
        if (!termsNotNull.isEmpty) termsNotNull.reduceLeft(_+_)
        else null

    }

    def fn_div_periods():BigDecimal = {

        val numerator = getArgument("NUMERATOR")
        val denominator:BigDecimal = BigDecimal(m.systime.obj.sysTimeKey.substring(5, 7))

        if (numerator != null && denominator != null && denominator != 0)
        new BigDecimal(numerator.bigDecimal.divide(denominator.bigDecimal, MathContext.DECIMAL128))
        else null

    }

    def fn_rem_months():BigDecimal = {
        Periods.mRemMonths
    }

    def fn_expression():BigDecimal = {

        m.ekpis.map(k.codKpi).parsedExpression.eval(u)

    }

    def fn_percentage():BigDecimal = {

        val numerator = getArgument("NUMERATOR")
        val denominator = getArgument("DENOMINATOR")

        if (numerator != null && denominator != null && denominator != 0)
        new BigDecimal(numerator.bigDecimal.divide(denominator.bigDecimal, MathContext.DECIMAL128)) * 100
        else null

    }

    def fn_division():BigDecimal = {

        val numerator = getArgument("NUMERATOR")
        val denominator = getArgument("DENOMINATOR")

        if (numerator != null && denominator != null && denominator != 0)
        new BigDecimal(numerator.bigDecimal.divide(denominator.bigDecimal, MathContext.DECIMAL128))
        else null

    }

    def fn_calc_bva():BigDecimal = {

        val act = getArgument("ACTUAL")
        val bgt = getArgument("BUDGET")
        
        if (act != null && bgt != null) 
        method_BVA(act, bgt)
        else null

    }

    def fn_calc_nba():BigDecimal = {

        val act = getArgument("ACTUAL")
        val bgt = getArgument("BUDGET")

        if (act != null && bgt != null)
        method_NBA(act, bgt)
        else null

    }

    def fn_calc_npl():BigDecimal = {

        val act = getArgument("ACTUAL")
        val bgt = getArgument("BUDGET")

        if (act != null && bgt != null)
        method_NPL(act, bgt)
        else null

    }

    def fn_diff_percent():BigDecimal = {

        val numeratorStartN = getArgument("DIFF_PER_NUM_START")
        val numeratorEndN   = getArgument("DIFF_PER_NUM_END")
        val denominatorN    = getArgument("DIFF_PER_NUM_REF")

        val numeratorStart  = if (numeratorStartN==null) BigDecimal("0") else numeratorStartN
        val numeratorEnd    = if (numeratorEndN==null) BigDecimal("0") else numeratorEndN
        val denominator     = if (denominatorN==null) BigDecimal("0") else denominatorN

        if (numeratorStart != null && numeratorEnd != null && denominator != null && denominator != 0)
        new BigDecimal((numeratorEnd.bigDecimal.subtract(numeratorStart.bigDecimal)).divide(denominator.bigDecimal, MathContext.DECIMAL128)) * 100
        else null

    }

    def fn_avg_int(period:String):BigDecimal = {

        var depoKpis = List("MS2101AIC","MS2101AIY","MS2102AIC","MS2102AIY",
                              "MS2103AIC","MS2103AIY","MS2104AIC","MS2104AIY",
                              "MS2105AIC","MS2105AIY","MS2222AIM","MS2222AIE")

        val adjN        = getArgument("ADJ")
        val interestN   = getArgument("INTEREST")
        val avgbalN     = getArgument("AVG_BALANCE")

        val adj         = if (adjN==null) BigDecimal("0") else adjN
        val interest    = if (interestN==null) BigDecimal("0") else interestN
        val avgbal      = if (avgbalN==null) BigDecimal("0") else avgbalN
        val days        = if (period=="YTD") m.misdays.obj.misDaysYtd else m.misdays.obj.misDays
        val deposits    = if (depoKpis.contains(k.codKpi)) true else false
        val d360        = BigDecimal("360")

        val interest100 = new BigDecimal(interest.bigDecimal) * 100

        if (adj != 0)
            if (!deposits)
 		new BigDecimal(interest.bigDecimal.divide(adj.bigDecimal, MathContext.DECIMAL128)) * 100
            else
 		(new BigDecimal(interest.bigDecimal.divide(adj.bigDecimal, MathContext.DECIMAL128)) * 100).abs
        else
            if (avgbal != 0  && days != 0)
 		if (!deposits)
                    (new BigDecimal(interest.bigDecimal.divide(avgbal.bigDecimal, MathContext.DECIMAL128)) * 100) * (new BigDecimal(d360.bigDecimal.divide(days.bigDecimal, MathContext.DECIMAL128)))
 		else
                    ((new BigDecimal(interest.bigDecimal.divide(avgbal.bigDecimal, MathContext.DECIMAL128)) * 100) * (new BigDecimal(d360.bigDecimal.divide(days.bigDecimal, MathContext.DECIMAL128)))).abs
            else
 		null

    }

    def fn_spread(period:String):BigDecimal = {

        val adj         = getArgument("ADJ")
        val interest    = getArgument("INTEREST")
        val avgbal      = getArgument("AVG_BALANCE")
        val ftp         = getArgument("FTP")

        val days        = if (period=="YTD") m.misdays.obj.misDaysYtd else m.misdays.obj.misDays
        val d360        = BigDecimal("360")

        val expr1 = if (interest != null && ftp != null) new BigDecimal(interest.bigDecimal.add(ftp.bigDecimal)) * 100
                    else null

        if (expr1 != null)
            if (adj != 0)
                new BigDecimal(expr1.bigDecimal.divide(adj.bigDecimal, MathContext.DECIMAL128))
            else
                if (avgbal != 0  && days != 0)
                    (new BigDecimal(expr1.bigDecimal.divide(avgbal.bigDecimal, MathContext.DECIMAL128))) * (new BigDecimal(d360.bigDecimal.divide(days.bigDecimal, MathContext.DECIMAL128)))
                else
                    null
        else
            null

    }

    def fn_eqrates_bva():BigDecimal = {

        val naf = getArgument("EQURAF")
        val nms = getArgument("EQURMS")
        val nsb = getArgument("EQURSB")

        val af:BigDecimal = if (naf == null) 0 else naf
        val ms:BigDecimal = if (nms == null) 0 else nms
        val sb:BigDecimal = if (nsb == null) 0 else nsb

        val caf:BigDecimal = if (af >= 100) 100 else 0
        val cms:BigDecimal = if (ms >= 100) 100 else 0
        val csb:BigDecimal = if (sb >= 100) 100 else 0

        val sum:BigDecimal = caf + cms + csb
        val three:BigDecimal = 3
        new BigDecimal(sum.bigDecimal.divide(three.bigDecimal, MathContext.DECIMAL128))

    }


}

