
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

object spark_hive {
  
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
option("url", "jdbc:sqlserver://192.168.100.223:1433;database=AAD").
option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
option("dbtable", "t_po_detail_comment").
option("user", "readonly").
option("password", "HJ#ric1!").load()

import sqlContext.implicits._
import hiveContext.implicits._

mrsSource09.registerTempTable("source_table")

hiveContext.sql(s"""
CREATE TABLE default.parq_test_spark (
po_detail_comment_id	int
,wh_id	string
,po_number	string
,line_number	string
,item_number	string
,schedule_number	int
,sequence	int
,comment_type	string
,comment_date	string
,comment_text	string
)stored as PARQUET location '/antuit/databases/testwrite3/hj' """)

hiveContext.sql(s"INSERT INTO TABLE default.parq_test_spark SELECT * FROM source_table")


/*
 val hjSource = sqlContext.load("jdbc", 
  Map(
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "url" -> "jdbc:sqlserver://192.168.100.223:1433;database=AAD",
  "user" -> "readonly",
  "password" -> "HJ#ric1!",
  "dbtable" -> "t_po_detail_comment"))
*/
  //val mrsDf1 = mrsSource09.unionAll(mrsSource61)
   
  // val mrsDf2 = mrsDf1.unionAll(mrsSourceMain)
       
  // import hiveContext.implicits._
   //import hiveContext.sql
	//	val res = addDeltaFirstTime(mrsDf1)
		//res.registerTempTable("mrs_test_data")
		
		//sqlContext.sql("insert overwrite table `accelos.mrs15_adj_trn` select * from  `mrs_test_data`")
  //res.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "|").option("quoteMode", "NONE").option("escape", "\\").save("/antuit/databases/testwrite3/"+table);
		//res.write.mode("overwrite").format("com.databricks.spark.csv").option("delimiter", "\u0001").option("quote", " ").save("/antuit/databases/testwrite3/"+table);
		//mrsSource09.write.mode("overwrite").format("parquet").save("/antuit/databases/testwrite3/hj");

  }
}