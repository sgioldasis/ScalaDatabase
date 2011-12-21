/*
 * RsProcessors.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package scaladatabase

import scala.actors.Actor
import scala.actors.Actor._

import campaigns._

case class rsSelectProcessor(cr:Reader) extends Actor {

    def act() {
        loop {
            react {
                case record(rs) => {
                        cr.rec(rs)
                        Main.countRecs += 1
                        reply()
                    }
                case eors => {
                        cr.end
                        reply()
                        exit()
                    }
            }
        }
    }
}

case class oldrsSelectProcessor(p:(java.sql.ResultSet)=>Unit,e:()=>Unit) extends Actor {

    def act() {
        loop {
            react {
                case record(rs) => {
                        p(rs)
                        Main.countRecs += 1
                        reply()
                    }
                case eors() => {
                        e()
                        reply()
                        exit()
                    }
            }
        }
    }
}

case class rsUpdateProcessor(p:(Int)=>Unit) extends Actor {

    def act() {
        loop {
            react {
                case resupd(rs) => {
                        p(rs)
                        reply()
                        exit()
                    }
            }
        }
    }
}



