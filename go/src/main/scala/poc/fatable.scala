package poc

object fatable {
  /*
  def process(targetDF : DataFrame, sourceDF: DataFrame,hiveContext:HiveContext)={
    
    targetDF.registerTempTable("targetTable")
		hiveContext.cacheTable("targetTable") 
					
		sourceDF.registerTempTable("sourceTable")
		hiveContext.cacheTable("sourceTable")
		
		val sourceReqColumn = hiveContext.sql("select tail_number,flight_number,scheduled_departure_time,actual_departure_time from sourceTable")

					//Filter records from source table where actual_departure time is null
					val actualDepNull = sourceReqColumn.filter("actual_departure_time is null")

					//Filter records from source table where actual_departure time is NOt null
					val actualDepNotNull = sourceReqColumn.filter("actual_departure_time is not null")

					//register it as temp table
					actualDepNull.registerTempTable("actualDepNullRecords")
					actualDepNotNull.registerTempTable("actualDepNotNullRecords")


					val getNULLMatchingrecords=hiveContext.sql("""select b.id
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
							,a.modified_date_time
							from actualDepNullRecords a JOIN targetTable b where 
							concat(
							trim(cast(a.tail_number as string)),
							trim(cast(a.flight_number as string))) =
							concat(
							trim(cast(b.tail_number as string)),
							trim(cast(b.flight_number as string)))
							and unix_timestamp(cast(a.scheduled_departure_time as timestamp)) - unix_timestamp(cast(b.scheduled_departure_time as timestamp)) <= 3600""")

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
							,a.modified_date_time from actualDepNotNull a JOIN targetTable b where 
							concat(
							trim(cast(a.tail_number as string)),
							trim(cast(a.flight_number as string))) =
							concat(
							trim(cast(b.tail_number as string)),
							trim(cast(b.flight_number as string)))
							and unix_timestamp(cast(a.actual_departure_time as timestamp)) - unix_timestamp(cast(b.actual_departure_time as timestamp)) <= 3600""")

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
							,a.modified_date_time from actualDepNullRecords a left JOIN targetTable b on 
							concat(
							trim(cast(a.tail_number as string)),
							trim(cast(a.flight_number as string))) =
							concat(
							trim(cast(b.tail_number as string)),
							trim(cast(b.flight_number as string)))
							where
							concat(
							trim(cast(b.tail_number as string)),
							trim(cast(b.flight_number as string))) is null""")


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
							,a.modified_date_time from actualDepNotNullRecords a left JOIN targetTable b on
							concat(
							trim(cast(a.tail_number as string)),
							trim(cast(a.flight_number as string))) =
							concat(
							trim(cast(b.tail_number as string)),
							trim(cast(b.flight_number as string)))
							where
							concat(
							trim(cast(b.tail_number as string)),
							trim(cast(b.flight_number as string))) is null""")

					val newRecords = getNULLNewrecords.unionAll(getNotNULLNewrecords)

					import org.apache.spark.sql.functions.udf
					val arr = udf(() => java.util.UUID.randomUUID().toString())
					val withUUID = newRecords.withColumn("id", Coalesce(df("id"), arr()))
					
					
					withUUID.registerTempTable("newRecords")

					hiveContext.sql("insert into table "+targetTable+" select * from newRecords")
    
  }
  
  */
}