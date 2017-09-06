import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import java.util.Calendar
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SparkSession



object sparkMultiThread {
  
  def getMinID(Table:String,hiveContext:HiveContext){

			var minID=0
					try{

					minID=hiveContext.sql("select count(*) from antuit_prod."+Table).collect()(0).getInt(0)
					println("|||| Table "+Table+" Count :"+minID+"")

					}catch{
					case _ : Throwable => println("|||| Source Table Connection Issue :"+Table+"")
					}

	}
  
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Spark Multi Threading Example").master("local").getOrCreate()


    //Using Threads
    import org.apache.spark.sql.functions._

    import spark.implicits._

//    case class SimpsonCharacter(name: String, actor: String, episodeDebut: String)
//
//    val employee = spark.range(0, 100).select($"id".as("employee_id"), (rand() * 3).cast("int").as("dep_id"), (rand() * 40 + 20).cast("int").as("age")).toDF()
//
//    val simpsonsDF = spark.sparkContext.parallelize(
//      SimpsonCharacter("Homer", "Dan Castellaneta", "Good Night") ::
//        SimpsonCharacter("Marge", "Julie Kavner", "Good Night") ::
//        SimpsonCharacter("Bart", "Nancy Cartwright", "Good Night") ::
//        SimpsonCharacter("Lisa", "Yeardley Smith", "Good Night") ::
//        SimpsonCharacter("Maggie", "Liz Georges and more", "Good Night") ::
//        SimpsonCharacter("Sideshow Bob", "Kelsey Grammer", "The Telltale Head") ::
//        Nil)

    for (a <- 0 until 20) {
      val thread = new Thread {
        override def run {
          spark.sparkContext.parallelize(getMinID("acl_c_doc_man",hiveContext))
          spark.sparkContext.parallelize(getMinID("mrs_customer_delta", hiveContext))
          spark.sparkContext.parallelize(getMinID("mrs_inv_mst_delta",hiveContext))
//          spark.sparkContext.parallelize(Array("jjj", "kkk", "lll")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file4")
//          spark.sparkContext.parallelize(Array("mmm", "nnn", "ooo")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file5")
//          spark.sparkContext.parallelize(Array("ppp", "qqq", "rrr")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file6")
//          spark.sparkContext.parallelize(Array("sss", "ttt", "uuu")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file7")
//          spark.sparkContext.parallelize(Array("aaa", "bbb", "ccc")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file8")
//          spark.sparkContext.parallelize(Array("ddd", "eee", "fff")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file9")
//          spark.sparkContext.parallelize(Array("ggg", "hhh", "iii")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file10")
//          spark.sparkContext.parallelize(Array("jjj", "kkk", "lll")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file11")
//          spark.sparkContext.parallelize(Array("mmm", "nnn", "ooo")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file12")
//          spark.sparkContext.parallelize(Array("ppp", "qqq", "rrr")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file13")
//          spark.sparkContext.parallelize(Array("sss", "ttt", "uuu")).toDF().write.format("parquet").mode("overWrite").save("/tmp/vgiri/file14")
//          simpsonsDF.write.format("parquet").mode("overwrite").save("/tmp/vgiri/simpsonsDF_files")
//          employee.createOrReplaceTempView("employee")
//          spark.table("employee").write.mode("overWrite").format("parquet").saveAsTable("emp")
        }

      }
      thread.start
    }

  }
  
}