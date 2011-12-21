/*
 * CampaignStore.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package campaigns

import scala.collection.mutable.HashMap
import scala.BigDecimal._
import BigDecimal.RoundingMode._

case class Store {

    val storeMap = new HashMap[String,KpiStore]

    def checkExists(storeName:String) = {
        synchronized {
        var returnStore:KpiStore = null
        if (storeMap.contains(storeName)) {
            returnStore = storeMap(storeName)
        }
        else
        {
            val newStore = new KpiStore(storeName)
            storeMap += storeName -> newStore
            returnStore = newStore
        }
        returnStore
        }
    }

    def insert(storeName:String, dimKey:String, valType:String, dValue:BigDecimal) = {
        synchronized {
        checkExists(storeName).insert(dimKey, valType, dValue)
        }
    }

    def retrieve(storeName:String, dimKey:String, valType:String):BigDecimal = {
        synchronized {
        checkExists(storeName).retrieve(dimKey, valType)
        }
    }

    def display() = {
        storeMap.foreach(a => a._2.display)
    }

}

case class KpiStore (storeName:String) extends BvaFunctions {

    val storeAct = new HashMap[String,BigDecimal]
    val storeBgt = new HashMap[String,BigDecimal]
    val storeBva = new HashMap[String,BigDecimal]
    val storeAdj = new HashMap[String,BigDecimal]

    def chooseStore(valType:String) = {
        valType match {
            case "ACT" => storeAct
            case "ADJ" => storeAdj
            case "BGT" => storeBgt
            case "BVA" => storeBva
            case _     => null
        }
    }

    def insert(storeKey:String, valType:String, value:BigDecimal) = {
        val store = chooseStore(valType)
        if (store!=null) 
        if (value!=null) {
//        if (!store.contains(storeKey))
            val roundedValue = value.bigDecimal.setScale(2, java.math.BigDecimal.ROUND_HALF_UP)
            val storeValue = new BigDecimal(roundedValue)
//            if (value==2.775) {
//                println("------> Value="+value+", roundedValue="+roundedValue+", storeValue="+storeValue)
//            }
            store += storeKey -> storeValue
//            store += storeKey -> value.setScale(2, RoundingMode.ROUND_HALF_UP)
        }
    }

    def retrieve(storeKey:String, valType:String):BigDecimal = {
        var value:BigDecimal = null
        val store = chooseStore(valType)
        if (store!=null) {
            if (valType == "ACT" && storeAdj.contains(storeKey))
              value = storeAdj(storeKey)
            else
            if (store.contains(storeKey))
              value = store(storeKey)
            else
              if (valType == "BVA") {
                val bvaValue = calcBva(storeName,storeKey,valType)
//                println("Calculated BVA="+bvaValue+" for "+storeName+"."+storeKey)
                val bvaValueRounded =
                if (bvaValue!=null) bvaValue.setScale(2, RoundingMode.ROUND_HALF_UP)
                else null
                insert(storeKey,valType,bvaValue)  // in order not to recalculate it
                value = bvaValueRounded // in order to get the rounded value
            }
        }
        value
    }

    def display() = {
        storeAct.foreach(a => Console.println("Store.ACT["+storeName+"]"+a))
        storeBgt.foreach(a => Console.println("Store.BGT["+storeName+"]"+a))
        storeBva.foreach(a => Console.println("Store.BVA["+storeName+"]"+a))
    }

}

