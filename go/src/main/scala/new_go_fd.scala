
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.apache.spark.storage.StorageLevel._

object new_go_fd {
  
 
  

 def addDeltaFirstTime(deltaDf: DataFrame): DataFrame = {
			
	            import org.apache.spark.sql.functions._ 
							deltaDf.withColumn("sequence", monotonically_increasing_id) 
	}
    
    def main(args: Array[String])
  {
    
		val conf = new SparkConf().setAppName("MRS")
		val sc = new SparkContext(conf)
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    val dbtable = args(0)
   	val table = dbtable.substring(dbtable.indexOf(".")+1)
		val db = dbtable.substring(0,dbtable.indexOf("."))
		val sourceTable =  table.substring(6)
    
		val mrsSource09 = hiveContext.read.format("jdbc").
option("url", "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0009SQLDBFacilityData09_001").
option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
option("dbtable", sourceTable).
option("user", "readonly").
option("password", "R3@60n1Y$").load()

  val mrsSource61 = hiveContext.read.format("jdbc").
option("url", "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0002SQLDBFacilityData61_001").
option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
option("dbtable", sourceTable).
option("user", "readonly").
option("password", "R3@60n1Y$").load()
  
   /*   val mrsSourceMain = sqlContext.load("jdbc", 
  Map(
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "url" -> "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0266SQLDBFacilityDataMain_001",
  "user" -> "readonly",
  "password" -> "R3@60n1Y$",
  "dbtable" -> table))
  */
   val mrsDf1 = mrsSource09.unionAll(mrsSource61)
   
  // val mrsDf2 = mrsDf1.unionAll(mrsSourceMain)
       
   
		val res = addDeltaFirstTime(mrsDf1)
    res.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\u0001").option("quoteMode", "NONE").save("/antuit/databases/testwrite3/"+table);


  }
  
}