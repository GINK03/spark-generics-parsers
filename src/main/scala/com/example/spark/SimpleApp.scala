package com.irep.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import scala.util.Try
import scala.util.control.Exception._

class CSVParser(header:Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def convert(key:String, array:Array[String]) = Try { 
     array(index(key))
  }
  def apply(key:String, array:Array[String]):String = {
     convert(key, array).getOrElse("___PARSE_ERR___")
  }
}
object SimpleCSV {
  def main(args: Array[String]) {
    val logFile = "/mnt/sdb1/hadoop/201702_huy_1.5m.csv"
    val conf    = new SparkConf().setAppName("Simple Parser")
    val sc      = new SparkContext(conf)
    val csv     = sc.textFile(logFile)
    val data    = csv map { x => 
       var isQuote:Boolean = false
       var escaped:Seq[Char] = Range(0, x.length - 1) map { i:Int => 
          var r:Char = x(i)
	  if( x(i) == ',' && x(i+1) == '"' ) { 
	    isQuote = true 
	  }else if( x(i) == ',' && isQuote == true ) {
	    r = '|'
	  }else if( x(i) == '"' && x(i+1) == ',') {
	    isQuote = false
	  }
	  r
       }
       Try { 
         escaped = escaped:+x(x.length - 1)
       }
       val escaped_s = escaped.mkString("")
       escaped_s.split(',') map { x2 => 
         x2.trim
       } 
    } 
    val parse  = new CSVParser(data.take(1)(0)) 
    val rows = data filter { x => 
      parse("tuuid", x) != "tuuid"
    }
    val tuuids = rows map { x => 
      parse("tuuid", x) 
    } 

    tuuids take(100000) map { x => 
      println(x)
    }
  }
}
