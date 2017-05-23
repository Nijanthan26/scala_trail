package com.lineage

import org.apache.spark.sql.DataFrame
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.lit
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import scala.io.Source
import java.sql.DriverManager
import java.sql.Connection
import org.apache.spark.sql.types.{StructType,StructField,StringType}
import org.apache.spark.sql.Row
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

object flg {
  /*
  
	def main(args: Array[String]): Unit = {
	
			val conf = new SparkConf().setAppName("DeltaAdd")
			val sc = new SparkContext(conf)
			val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc) 
			
			val targetTable = args(0)
			val table = targetTable.substring(targetTable.indexOf(".")+1)
			val sourceTable1 = args(1)
			val sourceTable2 = args(2)
			
			import org.joda.time.DateTime
			import org.joda.time.format.DateTimeFormat
			
			val maxModifiedDate = hiveContext.sql(" select max(modified_date_time) from "+targetTable).collect()(0).getString(0)
			val maxDate = maxModifiedDate.substring(0,maxModifiedDate.indexOf(" "))
			println("|||| Maxx date from target table"+maxDate)
			val parseDate = DateTime.parse(maxDate,DateTimeFormat.forPattern("yyyy-MM-dd"))
			val min2Months = (parseDate.minusMonths(2)).toString
			val requerDate = min2Months.substring(0,min2Months.indexOf("T"))
			println("|||| min 2 months of Maxx date from target table"+requerDate)
			
			//Fecth last two months data from target table
			val targetDF = hiveContext.sql(" select * from "+targetTable+" where modified_date_time >= "+requerDate)
			targetDF.printSchema
			targetDF.show
			
			targetDF.registerTempTable("targetTable")
			hiveContext.cacheTable("targetTable") 
			
			//Fetch last 1 hr data from source table
			val maxModifiedTime = hiveContext.sql(" select cast(max(modified_date_time) as timestamp) as maxdate from "+sourceTable1) //.collect()(0).getString(0)
			println("|||| Maxx date from source table"+maxModifiedTime.show)
			//Error modified time.select
			val requiredTime = maxModifiedTime.select(from_unixtime(unix_timestamp(col("maxdate")).minus(60 * 60), "YYYY-MM-dd HH:mm:ss"))
			println("|||| min 1 hour of Maxx date from source table"+requiredTime)
			val sourceDF = hiveContext.sql(" select * from "+targetTable+" where modified_date_time >= "+requiredTime)
			sourceDF.registerTempTable("sourceTable")
			hiveContext.cacheTable("sourceTable")
			sourceDF.printSchema
			sourceDF.show
			
			//Source Table with four required columns
			val sourceReqColumn = hiveContext.sql("select tail_number,flight_number,scheduled_departure_time,actual_departure_time from sourceTable")
			
			//Filter records from source table where actual_departure time is null
			val actualDepNull = sourceReqColumn.filter("actual_departure_time is null")
			
			//Filter records from source table where actual_departure time is NOt null
			val actualDepNotNull = sourceReqColumn.filter("actual_departure_time is not null")
		
			//register it as temp table
			actualDepNull.registerTempTable("actualDepNullRecords")
			actualDepNotNull.registerTempTable("actualDepNotNullRecords")
			
			
			val getNULLMatchingrecords=hiveContext.sql("""select
b.id
,a.fa_flight_id
,b.log_flight_id
,a.flight_number
,a.tail_number
,a.ltv_tail_number
,a.origin_airport
,a.departure_gate
,a.destination_airport
,a.arrival_gate
,a.scheduled_departure_time
,a.actual_departure_time
,a.scheduled_arrival_time
,b.actual_arrival_time
,a.modified_date_time from actualDepNullRecords a JOIN targetTable b
where a.tail_number=b.tail_number and a.flight_number=b.flight_number and 
a.scheduled_departure_time=b.scheduled_departure_time""")

val getNotNULLMatchingrecords=hiveContext.sql("""select
b.id
,a.fa_flight_id
,b.log_flight_id
,a.flight_number
,a.tail_number
,a.ltv_tail_number
,a.origin_airport
,a.departure_gate
,a.destination_airport
,a.arrival_gate
,a.scheduled_departure_time
,a.actual_departure_time
,a.scheduled_arrival_time
,b.actual_arrival_time
,a.modified_date_time from actualDepNullRecords a JOIN targetTable b
where a.tail_number=b.tail_number and a.flight_number=b.flight_number and 
a.actual_departure_time=b.actual_departure_time""") //actual departure time ?

			//get matching records from target table by joining DF with actual Dep time as null and not null 

val updatedRecords = getNULLMatchingrecords.unionAll(getNotNULLMatchingrecords)

updatedRecords.registerTempTable("updatedRecords")


hiveContext.sql("insert into table "+targetTable+" select * from updatedRecords")


val getNULLNewrecords = hiveContext.sql("""select 
a.fa_flight_id
,b.log_flight_id
,a.flight_number
,a.tail_number
,a.ltv_tail_number
,a.origin_airport
,a.departure_gate
,a.destination_airport
,a.arrival_gate
,a.scheduled_departure_time
,a.actual_departure_time
,a.scheduled_arrival_time
,b.actual_arrival_time
,a.modified_date_time from actualDepNullRecords a JOIN targetTable b 
where a.tail_number!=b.tail_number or a.flight_number!=b.flight_number or 
a.scheduled_departure_time!=b.scheduled_departure_time""")


			val getNotNULLNewrecords = hiveContext.sql("""select 
a.fa_flight_id
,a.flight_number
,a.tail_number
,a.ltv_tail_number
,a.origin_airport
,a.departure_gate
,a.destination_airport
,a.arrival_gate
,a.scheduled_departure_time
,a.actual_departure_time
,a.scheduled_arrival_time
,a.modified_date_time from actualDepNotNullRecords a JOIN targetTable b 
where a.tail_number!=b.tail_number or 
a.flight_number!=b.flight_number or a.scheduled_departure_time!=b.scheduled_departure_time""")
			
val newRecords = getNULLNewrecords.unionAll(getNotNULLNewrecords)
/*
import org.apache.spark.sql.functions.udf
val arr = udf(() => java.util.UUID.randomUUID().toString())
val withUUID = newRecords.withColumn("id", coalesce(newRecords("id"), arr()))
*/
newRecords.registerTempTable("newRecords")

hiveContext.sql("insert into table "+targetTable+" select * from newRecords")

	}
  */
}