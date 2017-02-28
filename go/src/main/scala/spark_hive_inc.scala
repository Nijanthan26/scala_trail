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

object spark_hive_inc {
  
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
    
		
				val mrsSource09 = hiveContext.read.format("jdbc").
option("url", "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0009SQLDBFacilityData09_001").
option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
option("dbtable", "adj_trn").
option("user", "readonly").
option("password", "R3@60n1Y$").load()

   val mrsSource61 = hiveContext.read.format("jdbc").
option("url", "jdbc:sqlserver://us0266sqlsrvmrs001.database.windows.net:1433;databaseName=US0002SQLDBFacilityData61_001").
option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver").
option("dbtable", "adj_trn").
option("user", "readonly").
option("password", "R3@60n1Y$").load()

import sqlContext.implicits._
import hiveContext.implicits._

val mrsSource=mrsSource09.unionAll(mrsSource61)

mrsSource.registerTempTable("source_table")

hiveContext.sql("""
create external table default.mrs_sqoopdest_table
(
ID	int
,SQLDATETIME	string
,FADJREASON	string
,FADJTYPE	string
,FBATCH	string
,FBDATE	string
,FCLAIMQTY	bigint
,FCUSTCODE	string
,FCUSTLOT	string
,FEDI	string
,FEDISNDDTE	string
,FGROSSWGT	bigint
,FLOCATION	string
,FLOT	string
,FNETWGT	bigint
,FNOTES	string
,FOEDI	string
,FOEDISNDTE	string
,FOWNER	string
,FPAL	bigint
,FPALLETID	string
,FPRODGROUP	string
,FPRODUCT	string
,FQTY	bigint
,FSEND	string
,FSEQUENCE	string
,FSERIAL	string
,FSTATUS	string
,FSUPLRPROD	string
,FTRACK	string
,FMARK	string
)stored as PARQUET location '/antuit/databases/testwrite3/mrs_sqoo_par'""")

hiveContext.sql("create table dummy1 like default.mrs_sqoopdest_table")
hiveContext.sql("create table dummy2 like default.mrs15_adj_trn_spark_par")


hiveContext.sql("INSERT into TABLE dummy1 SELECT * FROM source_table")

val newDF=hiveContext.sql("select * from dummy1")
val oldSeqDF=hiveContext.sql("select * from default.mrs15_adj_trn_spark_par")

val oldDF=oldSeqDF.drop("sequence")

val updateDF=newDF.except(oldDF)

val udpate=addDeltaIncremental(oldDF,updateDF,hiveContext)

udpate.registerTempTable("updated_records")

hiveContext.sql("insert into table dummy2 select * from updated_records")


hiveContext.sql("insert into table default.mrs15_adj_trn_spark_par select * from dummy1")


  }
  
}