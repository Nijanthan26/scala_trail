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

object spark_pull {
  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf().setAppName("Spark_POC")
		val sc = new SparkContext(conf)
		val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc) 
    
    val SourceData = hiveContext.read.format("jdbc").
		option("url", "jdbc:mysql://172.31.6.118:3306/testdb").
		option("driver", "com.mysql.jdbc.Driver").
		option("dbtable", "table1").
		option("user", "testuser").
		option("password", "abdul").load()
		
		SourceData.show
		
		SourceData.registerTempTable("source_table")
		
		hiveContext.sql("insert into default.sqooptable select * from source_table")
  
  }
  
}