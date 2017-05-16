package poc

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
import org.apache.spark.sql.functions.udf

object test {

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
					val parseDate = DateTime.parse(maxDate,DateTimeFormat.forPattern("yyyy-MM-dd"))
					val min2Months = (parseDate.minusMonths(2)).toString
					val requerDate = min2Months.substring(0,min2Months.indexOf("T"))

					//Fecth last two months data from target table
					val targetDF = hiveContext.sql(" select * from "+targetTable+" where modified_date_time >= "+requerDate)

					//Fetch last 1 hr data from source table
					val maxModifiedTime = hiveContext.sql(" select max(modified_date_time) as maxdate from "+sourceTable1).collect()(0).getString(0)
					//Error modified time.select
					val requiredTime = maxModifiedTime.select(from_unixtime(unix_timestamp(col("maxdate")).minus(60 * 60), "YYYY-MM-dd HH:mm:ss"))

					val sourceDF = hiveContext.sql(" select * from "+targetTable+" where modified_date_time >= "+requiredTime)
					
					fatable.process(targetDF,sourceDF,hiveContext)
		
					/************* Logic 2 : Read & Process Json data ************************************/
					//create view "start_of_flight1" on json data
					hiveContext.sql("""create view start_of_flight1 as select 
              flight_id,
							get_json_object(fjsonstring.msg_data, '$.start_of_flight.tail_number') as tail_number,
							get_json_object(fjsonstring.msg_data, '$.start_of_flight.flight_number') as flight_number,
							get_json_object(fjsonstring.msg_data, '$.start_of_flight.origination.icao') as org_airport,
							get_json_object(fjsonstring.msg_data, '$.start_of_flight.destination.icao') as dest_airport,
							get_json_object(fjsonstring.msg_data, '$.start_of_flight.date_time') as actual_departure_time
							from fjsonstring where msg_type="StartOfFlight_json"""")
					
					//create view "end_of_flight1" on json data
					hiveContext.sql("""create view end_of_flight1 as select
              flight_id,
							get_json_object(fjsonstring.msg_data, '$.start_of_flight.tail_number') as tail_number,
							get_json_object(fjsonstring.msg_data, '$.start_of_flight.flight_number') as flight_number,
							get_json_object(fjsonstring.msg_data, '$.start_of_flight.origination.icao') as org_airport,
							get_json_object(fjsonstring.msg_data, '$.start_of_flight.destination.icao') as dest_airport,
							get_json_object(fjsonstring.msg_data, '$.start_of_flight.date_time') as actual_arrival_time
							from fjsonstring where msg_type="EndOfFlight_json"""")
					
					//compare both view get matching records
					val jsonMatching = hiveContext.sql("""select
							a.actual_departure_time,
							b.actual_arrival_time,
							a.flight_number,
              a.flight_id,
							a.tail_number
              from start_of_flight1 a JOIN end_of_flight1 b 
              concat(
							trim(cast(a.tail_number as string)),
							trim(cast(a.flight_number as string)),
              trim(cast(a.org_airport as string)),
              trim(cast(a.dest_airport as string))) =
							concat(
							trim(cast(b.tail_number as string)),
							trim(cast(b.flight_number as string)),
              trim(cast(a.org_airport as string)),
              trim(cast(a.dest_airport as string)))""")
							//and unix_timestamp(cast(a.scheduled_departure_time as timestamp)) - unix_timestamp(cast(b.scheduled_departure_time as timestamp)) <= 5400""")
						
           jsonMatching.registerTempTable("jsonData")
           
           //compare  matching records with target table
           val getfinalRecord=hiveContext.sql("""select a.id
							,a.fa_flight_id
							,b.flight_id as log_flight_id
							,a.flight_number
							,a.tail_number
							,a.ltv_tail_number
							,a.origin_airport
							,a.departure_gate
							,a.destination_airport
							,a.arrival_gate
							,a.scheduled_departure_time
							,b.actual_departure_time
							,a.scheduled_arrival_time
							,b.actual_arrival_time
							,a.modified_date_time
							from targetTable a JOIN jsonData b where 
							concat(
							trim(cast(a.tail_number as string)),
							trim(cast(a.flight_number as string))) =
							concat(
							trim(cast(b.tail_number as string)),
							trim(cast(b.flight_number as string)))
							and unix_timestamp(cast(a.actual_departure_time as timestamp)) - unix_timestamp(cast(b.actual_departure_time as timestamp)) <= 3600""")
							
						getfinalRecord.registerTempTable("FinalLogData")
						
						hiveContext.sql("insert into table "+targetTable+" select * from FinalLogData")

							}

							}