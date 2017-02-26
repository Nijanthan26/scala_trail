
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import com.microsoft.sqlserver.jdbc.SQLServerDriver
import org.apache.spark.storage.StorageLevel._
import java.security.MessageDigest
import org.apache.spark.sql.Dataset

object new_go {
  
def addDeltaIncremental(initialDfShaWithDate: DataFrame, deltaDf: DataFrame, hiveContext:HiveContext): DataFrame = {
		    	val initialDfSha = initialDfShaWithDate//.drop("archive_date")
				val  delta = deltaDf
				
					initialDfShaWithDate.registerTempTable("initialDfSha")
					val currentRowNum = hiveContext.sql("select max(sequence) from initialDfSha").collect()(0).getLong(0)
					delta.registerTempTable("deltaDfSha")
					import org.apache.spark.sql.functions._ 
					val deltaDfShaSeq = delta.withColumn("sequence", monotonically_increasing_id + currentRowNum)
	return deltaDfShaSeq
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
option("dbtable", sourceTable).
option("user", "readonly").
option("password", "R3@60n1Y$").load()

  val mrsSource61 = sqlContext.read.format("jdbc").
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
   val DF1 = hiveContext.sql("SELECT * FROM "+dbtable)
   
      val col=DF1.columns
      val colLength=DF1.columns.length - 1
      val oldDf=DF1.drop(col(colLength))
   
  // val mrsDf2 = mrsDf1.unionAll(mrsSourceMain)
      
     val mrsSource=mrsSource09.unionAll(mrsSource61)
       
   mrsSource.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\u0001").save("/antuit/sqoopdest/"+table)
  // mrsSource61.write.mode("append").format("com.databricks.spark.csv").option("delimiter", "\u0001").save("/antuit/sqoopdest/"+table);
   
   
   val newDf=hiveContext.sql("SELECT * FROM accelos."+table) 
   
   val updatedDf=newDf.except(oldDf)
   
   updatedDf.write.saveAsTable("accelos.update_mrs") //for testing
   
   val updateSeq=addDeltaIncremental(DF1,updatedDf,hiveContext)
   
  // updatedDf.write.mode("append").format("com.databricks.spark.csv").option("delimiter", "|").save("/antuit/databases/testwrite3/"+table);
   updateSeq.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "|").save("/antuit/databases/testwrite3/"+table);
				
	//	res.write.format("orc").saveAsTable(dbtable);  //Change  schema and table name

  }
  
}