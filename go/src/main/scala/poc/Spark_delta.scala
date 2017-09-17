package poc

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.functions.lit

object Spark_delta {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Spark_POC")
    // Initializing Spark Context, heart of the program which loads all the required drivers and config files
		val sc = new SparkContext(conf)
    // Createing HiveContext out of spark context to make connection to Hive Metastore.
		val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc) 
    
    // Creating JDBC connection to RDBMS using hive Context and pulling required table
    val SourceData = hiveContext.read.format("jdbc").
		option("url", "jdbc:mysql://172.31.6.118:3306/testdb").
		option("driver", "com.mysql.jdbc.Driver").
		option("dbtable", "table1").
		option("user", "testuser").
		option("password", "abdul").load()
		
		// Display the schema and data in the dataframe
		//SourceData.show
		
		// Register the dataframe as temporary table, so that we can query the data in dataframe.
		//SourceData.registerTempTable("source_table")
		
		//hiveContext.sql("insert into default.sqooptable select * from source_table")
		
		//select data from hive table in Datalake using Hive Context
		val DL = hiveContext.sql("select * from default.sqooptable")
		
		// Perform except between two different dataframe ( source df & datalake Df) to get the updated and newly added records in source df
		val updates=SourceData.except(DL)
		
		updates.registerTempTable("update_table")
		
		hiveContext.sql("create table default.dummy like default.sqooptable")
		hiveContext.sql("insert into default.dummy select * from update_table")
		hiveContext.sql("insert into default.sqooptable select * from update_table")
		hiveContext.sql("drop table if exists default.dummy")
  
  }
  
}