
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object go {
  
  
  
   def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("MRS Import")
    val sc = new SparkContext(conf)
    
    //sc.hadoopConfiguration.setInt( "mapreduce.input.fileinputformat.split.minsize",5242880)
    //sc.hadoopConfiguration.setInt( "mapreduce.input.fileinputformat.split.maxsize",5242880)
    
    import org.apache.spark.SparkContext._
    
    val fs = FileSystem.get(sc.hadoopConfiguration)
    
    val dirSize = fs.getContentSummary(/user/hive/warehouse/hj_t_item_master).getLength
    val fileNum = dirSize/(5 * 1024 * 1024)  

     
   // val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
    
    val dfProc = hiveContext.sql("select * from antuit_stage.hj_t_item_master")
    
    dfProc.coalesce(fileNum).write.format("orc").saveAsTable("default.hj_t_bmm_dummy")


   } 
}