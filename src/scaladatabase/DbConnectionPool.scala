/*
 * DbConnectionPool.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package scaladatabase

import scala.actors.Actor
import scala.actors.Actor._

case class msg(op:String, q:String, p:Actor)

object conPool extends Actor {

    val maxConnections = 8
    val pDbType = Main.aDbType
    val pOdbc = Main.aDatabase

    var arConPool = new Array[dbConnection](maxConnections)
    var curConnection = 0

    def nextConnection() {
        curConnection = curConnection + 1
        if (curConnection == maxConnections) curConnection = 0
        //        println("curConnection="+curConnection)
    }

    def dbCreateConnections() = {
        for (i <- 0 until maxConnections) {
            arConPool(i) = new dbConnection(i+1,pDbType,pOdbc)
            arConPool(i).start
            println("Created and Started Connection"+i)
        }
    }

    def act() {
        dbCreateConnections()
        loop {
            react {
                case msg(op, q, p) => {arConPool(curConnection) ! msg(op, q, p);nextConnection()}
            }
        }

    }
}

