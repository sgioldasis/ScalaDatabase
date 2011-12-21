/*
 * Connection.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package scaladatabase

import scala.actors.Actor
import scala.actors.Actor._

import java.util.Properties._

case class dbStatus
case object Idle extends dbStatus
case object Busy extends dbStatus

case class record(rs:java.sql.ResultSet)
case class eors
case class resupd(rs:Int)


case class dbConnection (id:Int, pDbType:String, pOdbc:String) extends Actor {

    val pUrl_NATIVEIQ       = "jdbc:sybase:Tds:";
    val pUrl_NATIVEIQ_PROD  = "172.26.100.74:2680";
    val pUrl_NATIVEIQ_DEV   = "172.26.100.153:2670";
    val pDriver_NATIVEIQ    = "com.sybase.jdbc3.jdbc.SybDriver";
    val pUserId_SYBASEIQ    = "DBA";
    val pPasswd_SYBASEIQ    = "SQL";

    val pUrl_MYSQL       = "jdbc:mysql://192.168.1.15:3306/dashboard?user=root&password=admin";
    val pDriver_MYSQL    = "com.mysql.jdbc.Driver";

    var pUrl    = "";
    var pDriver = "";
    var pUserId = "";
    var pPasswd = "";

    var dbConnection: java.sql.Connection = null;
    var status = Idle

    def dbLoadDriver() = {
        try {
            java.lang.Class.forName(pDriver)
        }
        catch  { case x =>
                Console.println("Unable to load the driver class! " + x.getMessage());
        }
    }

    def dbCreateConnection() = {
        try {
            if (pDbType.equalsIgnoreCase("NATIVEIQ")) {

                val props = new java.util.Properties();
                props.put("user", pUserId_SYBASEIQ);
                props.put("password", pPasswd_SYBASEIQ);
                props.put("CHARSET", "cp1253");

                dbConnection = java.sql.DriverManager.getConnection(pUrl, props);

            } else {
                dbConnection = java.sql.DriverManager.getConnection(pUrl);
            }
        } catch { case x =>
                Console.println("Couldn’t get connection: " + pOdbc + " - Message: " + x.getMessage());
        }
    }

    def dbOpenConnection() = {
        if (pDbType.equalsIgnoreCase("NATIVEIQ")) {
            pUrl = pUrl_NATIVEIQ;
            if (pOdbc.equalsIgnoreCase("CBOGIQ_PROD")) {
                pUrl += pUrl_NATIVEIQ_PROD;
            } else {
                pUrl += pUrl_NATIVEIQ_DEV;
            }
            pDriver = pDriver_NATIVEIQ;
            pUserId = pUserId_SYBASEIQ;
            pPasswd = pPasswd_SYBASEIQ;
        }
        if (pDbType.equalsIgnoreCase("MYSQL")) {
            pUrl    = pUrl_MYSQL;
            pDriver = pDriver_MYSQL;
        }
        dbLoadDriver();
        Console.println("Connection "+id+": Loaded driver class! ");
        dbCreateConnection();
        Console.println("Connection "+id+": Created Connection! ");
    }

    def act() {
        dbOpenConnection()
        loop {
            react {
                case msg("SELECT",q,p) => {
                        p.start
                        Console.println("Connection "+id+": Processing SELECT message: "+q)
                        val stmt = dbConnection.createStatement()
                        val rs = stmt.executeQuery(q)
                        while (rs.next) {
                            p !? record(rs)
                        }
                        p !? eors
                    }
                case msg("UPDATE",q,p) => {
                        p.start
                        Console.println("Connection "+id+": Processing UPDATE message: "+q)
                        val stmt = dbConnection.createStatement()
                        val rs = stmt.executeUpdate(q)
                        p !? resupd(rs)
                    }
                case msg(_,q,p) => Console.println("Connection "+id+": Processing OTHER message: "+q)
            }
        }
    }



}
