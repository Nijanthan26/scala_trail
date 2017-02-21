
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

object go {
  
  
  
   def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("MRS Import")
    val sc = new SparkContext(conf)
    
    val dbtable = args(0)
   	val table = dbtable.substring(dbtable.indexOf(".")+1)
		val db = dbtable.substring(0,dbtable.indexOf("."))
    
		val sourceTable =  table.substring(3)
    
    //sc.hadoopConfiguration.setInt( "mapreduce.input.fileinputformat.split.minsize",5242880)
    //sc.hadoopConfiguration.setInt( "mapreduce.input.fileinputformat.split.maxsize",5242880)
    
    import org.apache.spark.SparkContext._
    
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
   
        val hjSource = hiveContext.load("jdbc", 
  Map(
  "driver" -> "com.microsoft.sqlserver.jdbc.SQLServerDriver",
  "url" -> "jdbc:sqlserver://192.168.100.223:1433;database=AAD",
  "user" -> "readonly",
  "password" -> "HJ#ric1!",
  "dbtable" -> sourceTable))
      
   val DF = hiveContext.sql("SELECT * FROM antuit_stage."+table)
   //val OldDF = hiveContext.sql("SELECT * FROM antuit_stage."+table)
  
    val DF1=DF.drop("sha2")
    
    val DF2=DF1.drop("sequence")
    
    val oldDF=DF2.drop("archive_date")
    
    hjSource.write.saveAsTable("default.hj_source_item_master")
    
    val upsertsDF = hjSource.except(oldDF)
    
    upsertsDF.write.saveAsTable("default.upserts_item_master")

   } 
}