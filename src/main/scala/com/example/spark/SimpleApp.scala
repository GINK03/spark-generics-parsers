package com.irep.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import scala.util.Try
import scala.util.control.Exception._
/*
date_time,ip,request_uri,ipao97_value,referer,useragent,tuuid,account_id,data_owner_id,os,os_version,browser,browser_version,url_category_ids,event_ids,keyword_group_ids,keywords,segment_ids,gender_age,income,marriage,occupation,frequency_of_ec_buying,amount_of_ec_buying,brand_vs_price_oriented,children_adult,children_university,children_high_school,children_middle_school,children_elementary_school,children_preschooler,work_location,home_location,work_location_zipcode,home_location_zipcode,carrier,time
*/
class CSVParser(header:Array[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def convert(keys:Array[String], array:Array[String]) = Try { 
     keys map { x =>
       array(index(x))
     } toList
  }
  def convert(key:String, array:Array[String]) = Try { 
     array(index(key))
  }
  def apply(keys:Array[String], array:Array[String]):List[String] = {
     convert(keys, array).getOrElse( List.fill(keys.length)("___PARSE_ERR___") )
  }
  def apply(key:String, array:Array[String]):String = {
     convert(key, array).getOrElse("___PARSE_ERR___")
  }
}
object SimpleCSV {
  def getData(logFile:String) = {
    val conf    = new SparkConf().setAppName("Simple Parser").set("spark.driver.allowMultipleContexts", "true")
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
    val rows   = data filter { x => 
      parse("tuuid", x) != "tuuid"
    }
    (parse, rows)
  }
  def main(args: Array[String]) {
    val pr = getData("/mnt/sdb1/hadoop/201702_huy_0.5m.csv")
    val pr2 = getData("/mnt/sdb1/hadoop/201702_huy_1.5m.csv")
    val parse = pr._1
    val rows  = pr._2
    //val rows2 = pr2._2
    val tuuids = rows map { x => 
      parse("tuuid", x) 
    }
    tuuids take(100) map { x => 
      println(x)
    }
    /*
    val tuuid_ip_requesturi = rows map { x => 
      parse(Array("tuuid", "ip", "request_uri"), x)
    } filter { x => 
      //x(0) == "10582c4c-3672-4996-9815-bc1b0a420f92"
      x(2).matches("""cosmowater""")
    }

    tuuid_ip_requesturi take(10) map { x => 
      println(x)
    }*/
  }
}
