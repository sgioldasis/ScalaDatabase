/*
 * FileHelper.scala
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package scalafiles

import java.io._

class FileHelper(file : File) {

  var fw:FileWriter = new FileWriter(file)

  def close = {
      fw.close
  }
  def write(text : String) : Unit = {
    try {fw.append(text)}
    
  }
  def foreachLine(proc : String=>Unit) : Unit = {
    val br = new BufferedReader(new FileReader(file))
    try{ while(br.ready) proc(br.readLine) }
    finally{ br.close }
  }
  def deleteAll : Unit = {
    def deleteFile(dfile : File) : Unit = {
      if(dfile.isDirectory){
        val subfiles = dfile.listFiles
        if(subfiles != null)
          subfiles.foreach{ f => deleteFile(f) }
      }
      dfile.delete
    }
    deleteFile(file)
  }
}

//object FileHelper{
//  implicit def file2helper(file : File) = new FileHelper(file)
//}

//
// Examples of Use
//
//val dir = new File("/tmp/mydir/nested_dir")
//dir.mkdirs
//val file = new File(dir, "myfile.txt")
//file.write("one\ntwo\nthree")
//file.foreachLine{ line => println(">> " + line) }
//dir.deleteAll

