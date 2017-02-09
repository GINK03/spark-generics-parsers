package com.irep.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import scala.util.Try
import scala.util.control.Exception._
import scala.io.Source
import scala.collection.mutable.Map
import org.apache.log4j.Logger
import org.apache.log4j.Level

/*
date_time,ip,request_uri,ipao97_value,referer,useragent,tuuid,account_id,data_owner_id,os,os_version,browser,browser_version,url_category_ids,event_ids,keyword_group_ids,keywords,segment_ids,gender_age,income,marriage,occupation,frequency_of_ec_buying,amount_of_ec_buying,brand_vs_price_oriented,children_adult,children_university,children_high_school,children_middle_school,children_elementary_school,children_preschooler,work_location,home_location,work_location_zipcode,home_location_zipcode,carrier,time
*/
class CSVParser(header:List[String]) extends Serializable {
  val index = header.zipWithIndex.toMap
  def convert(keys:List[String], array:List[String]) = { 
     keys map { x =>
       array(index(x))
     } toList
  }
  def convert(key:String, array:List[String]) = Try { 
     array(index(key))
  }
  def apply(keys:List[String], array:List[String]):Option[List[String]] = {
     try { 
       Some(convert(keys, array) ) 
     } catch { 
       case e:Exception => None   
     }
  }
  def apply(key:String, array:List[String]):String = {
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
    val d_parse  = new CSVParser(data.take(1)(0) . toList) 
    val d_rows   = data filter { x => 
      d_parse("tuuid", x . toList) != "tuuid"
    }
    (d_parse, d_rows)
  }

  def getMasterData() = {
    val data = Source.fromFile("/mnt/sdb1/hadoop/old/master_data_owner.csv").getLines . toList . map { x => 
      x.split(',') . map { x2 => 
        x2 
      } . toList
    }
    var m:Map[String, String] = Map()
    val parse  = new CSVParser(data.take(1)(0)) 
    val rows   = data . filter { x => 
      parse("data_owner_id", x) != "data_owner_id"
    } . toList . map { x => 
      val d_n = parse(List("data_owner_id", "name"), x) 
      if( d_n != None) {
        m += (d_n . get(0) -> d_n . get(1) )
      }
    }
    m
  }
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val fileName = args(0) 
    val pr      = getData(f"/mnt/sdb1/hadoop/$fileName%s")
    val parse   = pr._1
    val row     = pr._2
    val tuuids  = row . map { x => 
      parse("tuuid", x . toList) 
    }
    val m = getMasterData()
    val res = row . map { x => 
      val multi = parse(List("data_owner_id", "tuuid", "date_time", "request_uri"), x . toList) 
      /*if( multi != None && m.get(multi .get (0)) !=  None ) {
        val key = multi.get(0) 
        multi.get ::: List(m.get(key).get)
      } else {
        None
      }*/
      /*if( multi != None && multi.get(3).matches(""".*cosmowater.*""") ) { 
        true
      } else {
        None
      }*/
      if( multi != None && multi.get(1) == "10582c4c-3672-4996-9815-bc1b0a420f92") { 
        true
      } else {
        None
      }
    } . filter { x => 
      x != None
    } . take(1000000) . map { x => 
      x
    } . toList
    res . map { x => 
      val tmp:Any = x 
      //println(x)
      //val tmp = x . productIterator . toList
      //print("A", x . productIterator . toList, "\n")
    }
  }
}
