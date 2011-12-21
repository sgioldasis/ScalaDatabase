/*
 * Functions_Ckpi.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package campaigns

import java.math.MathContext;
import scala.collection.mutable.HashMap

trait CkpiFunctions extends CkpiBasics {

    def select_cfn = {
        ckpi.compFunction match {
            case "GRP_COPY"     => (cfnDef("GRPVAL"),&cfn_grpval,null)
            case "MAXIMUM"      => (cfnDef("GRPVAL"),&cfn_maximum,null)
            case "MINIMUM"      => (cfnDef("GRPVAL"),&cfn_minimum,null)
            case "COUNT_EMPTY"  => (cfnDef("GRPVAL"),&cfn_countempty,null)
            case "QUARTER"      => (cfnDef("MBRVAL"),null,&mfn_quarter)
            case "Q1FLOOR"      => (cfnDef("MBRVAL"),null,&mfn_q1floor)
            case "RANK_REL"     => (cfnDef("MBRVAL"),null,&mfn_rankrel)
            case "TOPBOT10"     => (cfnDef("MBRVAL"),null,&mfn_topbot10)
            case _              => (cfnDef("UKNOWN"),null,null)
        }
    }

    def mfn_quarter():List[DimVal] = {
        hlp_rankBucket(4,"BUCKETS","NORMAL")
    }

    def mfn_q1floor():List[DimVal] = {
        hlp_rankBucket(4,"B1FLOOR","NORMAL")
    }

    def mfn_rankrel():List[DimVal] = {
        hlp_rankBucket(1,"RANKREL","NORMAL")
    }

    def mfn_topbot10():List[DimVal] = {

        var top10:DimVal = null
        var bot10:DimVal = null
        var listTop10:List[DimVal] = List()
        var listBot10:List[DimVal] = List()
        var setTop10:Set[String] = Set()
        var setBot10:Set[String] = Set()

        def findTenth(listRanked:List[DimVal]) = {
            var aa = 0
            var dv10:DimVal = null
            for (dv <- listRanked) {
                aa = aa + 1
                if (aa == 10) dv10 = dv
            }
            dv10
        }

        def topbot10Value(dv:DimVal) = {
            if (setTop10.contains(dv.u)) DimVal(dv.u,1)
            else if (setBot10.contains(dv.u)) DimVal(dv.u,2)
            else DimVal(dv.u,null)
        }

        val listRankNormal=hlp_rankBucket(1,"RANKS","NORMAL").filter(dv => dv.v!=null).sort(_<_)
        val listRankInverse=hlp_rankBucket(1,"RANKS","INVERSE").filter(dv => dv.v!=null).sort(_<_)

        top10 = findTenth(listRankNormal)
        bot10 = findTenth(listRankInverse)

        listTop10 = listRankNormal.filter(dv => dv <= top10)
        listBot10 = listRankInverse.filter(dv => dv <= bot10)

        listTop10.foreach(dv => setTop10 += dv.u)
        listBot10.foreach(dv => setBot10 += dv.u)
        println("setTop10: "+setTop10+", setBot10: "+setBot10)

        getMemberDimVals map topbot10Value
    }

    def hlp_rankBucket(numBuckets:Int, sResult:String, rankDir:String):List[DimVal] = {

        val listDimVals                       = getMemberDimVals
        val normalRankDir                     = if (rankDir=="INVERSE") false else true
        var maxBucket:BigDecimal              = numBuckets

        var setValues:Set[BigDecimal]         = Set()
        var listValuesSorted:List[BigDecimal] = List()

        val mapBucketRankFloors               = new HashMap[BigDecimal, BigDecimal]
        val mapBucketValueFloors              = new HashMap[BigDecimal, BigDecimal]
        val mapValueRank                      = new HashMap[BigDecimal, BigDecimal]
        val mapValueBucket                    = new HashMap[BigDecimal, BigDecimal]

        def createSetValues = {
            listDimVals.foreach(dv => if (dv.v!=null) setValues += dv.v)
        }

        def maxRank:BigDecimal = {
            setValues.size
        }

        def createListSorted = {
            listValuesSorted =
            if (normalRankDir) setValues.toList.sort(_>_)
            else setValues.toList.sort(_<_)

        }

        def createMapBucketRankFloors = {
            if (maxRank < maxBucket) maxBucket = maxRank

            if (maxBucket != 0) {
                val rankStep = maxRank.bigDecimal.divide(maxBucket.bigDecimal,MathContext.DECIMAL128)
                //            println("rankStep="+rankStep)

                for (b <- 1 to maxBucket.intValue) {
                    val bb:BigDecimal = b
                    val bucketRankFloorU = rankStep.multiply(bb.bigDecimal, MathContext.DECIMAL128)
                    val bucketRankFloor =  new BigDecimal(bucketRankFloorU.setScale(0, BigDecimal.RoundingMode.ROUND_HALF_UP.id))
                    mapBucketRankFloors += bb -> bucketRankFloor
                    //                println("mapBucketRankFloors["+bb+"]="+bucketRankFloor)
                }
            }
        }

        def rankBucket = {
            createSetValues
            createMapBucketRankFloors
            //            println("rankBucket.mapBucketRankFloors= "+mapBucketRankFloors)
            createListSorted

            var r:BigDecimal = 0
            var b:BigDecimal = 1

            for (v <- listValuesSorted) {
                r = r + 1
                mapValueRank += v -> r
                //                println("mapValueRank["+v+"]="+r)

                if (r > mapBucketRankFloors(b)) b = b + 1

                mapValueBucket += v -> b
                //                println("mapValueBucket["+v+"]="+b)

                mapBucketValueFloors += b -> v
                //                println("mapBucketValueFloors["+b+"]="+v)

            }
        }

        def calcBuckets = {
            rankBucket
            for (dv <- listDimVals) yield 
            if (dv.v != null && mapValueBucket.contains(dv.v))
            DimVal(dv.u, mapValueBucket(dv.v))
            else
            DimVal(dv.u, null)
        }

        def calcB1Floor = {
            rankBucket
            for (dv <- listDimVals) yield
            if (mapBucketValueFloors.contains(1))
            DimVal(dv.u, mapBucketValueFloors(1))
            else
            DimVal(dv.u, null)
        }

        def calcRanks = {
            rankBucket
            for (dv <- listDimVals) yield
            if (dv.v != null && mapValueRank.contains(dv.v))
            DimVal(dv.u, mapValueRank(dv.v))
            else
            DimVal(dv.u, null)
        }

        def calcRankRel = {
            rankBucket
            for (dv <- listDimVals) yield
            if (dv.v != null && mapValueRank.contains(dv.v))
            DimVal(dv.u, mapValueRank(dv.v))
            else
            DimVal(dv.u, maxRank+1)
        }

        def calcNulls = {
            for (dv <- listDimVals) yield DimVal(dv.u, null)
        }

        // Main
        // hlp_rankBucket(numBuckets:Int, sResult:String, rankDir:String)

        //        var result:List[DimVal] = List()
        sResult match {
            case "BUCKETS"      => calcBuckets
            case "B1FLOOR"      => calcB1Floor
            case "RANKS"        => calcRanks
            case "RANKREL"      => calcRankRel
            case _              => calcNulls
        }
        
        //        println("hlp_rankBucket["+sResult+"], result= "+result)
        //        result

    }

    def cfn_grpval():BigDecimal = {
        ckpi.refKpi.value(gName)
    }

    def cfn_maximum():BigDecimal = {
        val listNotNull = getMemberValues.filter{i => i != null}
        if (listNotNull.isEmpty) null
        else listNotNull.reduceLeft((s1, s2) => if (s1 > s2) s1 else s2)
    }

    def cfn_minimum():BigDecimal = {
        val listNotNull = getMemberValues.filter{i => i != null}
        if (listNotNull.isEmpty) null
        else listNotNull.reduceLeft((s1, s2) => if (s1 < s2) s1 else s2)
    }

    def cfn_countempty():BigDecimal = {
        getMemberValues.foldLeft(0)((cnt, elm) => if (elm == null || elm <= 0) cnt+1 else cnt)
    }


}


