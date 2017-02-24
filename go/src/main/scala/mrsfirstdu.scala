

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.apache.spark.storage.StorageLevel._


object mrsfirstdu {
  
  
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
    
		val mrsSource09 = sqlContext.read.format("jdbc").
option("url", "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0009SQLDBFacilityData09_001").
option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
option("dbtable", "adj_trn").
option("user", "readonly").
option("password", "R3@60n1Y$").load() 

val source=hiveContext.sql("select * from antuit_stage.mrs15_adj_trn_temp")

val SourcSeq = source.drop("sequence")
		 /*
    val mrsSource09 = sqlContext.read.format("jdbc").options( 
  Map(
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "url" -> "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0009SQLDBFacilityData09_001",
  "user" -> "readonly",
  "password" -> "R3@60n1Y$",
  "dbtable" -> sourceTable)).load()
  
  
      val mrsSource61 = sqlContext.read.format("jdbc").options(  
  Map(
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "url" -> "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0002SQLDBFacilityData61_001",
  "user" -> "readonly",
  "password" -> "R3@60n1Y$",
  "dbtable" -> sourceTable)).load()
 
   val mrsSourceMain = sqlContext.load("jdbc", 
  Map(
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "url" -> "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0266SQLDBFacilityDataMain_001",
  "user" -> "readonly",
  "password" -> "R3@60n1Y$",
  "dbtable" -> table))
 
		
	val url = "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0009SQLDBFacilityData09_001"
  val username = "readonly"
	val password = "R3@60n1Y$"
	
	val prop = new java.util.Properties
	
						prop.setProperty("user",username)
					prop.setProperty("password",password)
	 */
  // val mrsDf1 = mrsSource09
   
  // val mrsDf2 = mrsDf1.unionAll(mrsSourceMain)
      mrsSource09.count()
   
	//	val res = addDeltaFirstTime(mrsDf1)
	//	res.count()
		
		import org.apache.spark.sql.types._
    val schema = StructType(SourcSeq.schema.fields)
    val df = mrsSource09.rdd
		val srcDelta = sqlContext.createDataFrame(df,schema )
		
		val res = srcDelta.except(SourcSeq)
		
		res.saveAsTable("accelos.adj_trn_temp")
		
	
		
		//res.write.mode("append").format("orc").option("delimiter", "\t").save("/antuit/sqoopdest/mrs15_adj_trn"); 
		//dfDeltatx.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "|").save("/antuit/sqoopdest/mrs15_adj_trn");
		//dfDeltatx.write.mode("overwrite").saveAsTable("accelos.adj_trn_test");

  }
  
}