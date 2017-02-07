
import org.apache.spark.SparkContext
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import java.security.MessageDigest
import org.apache.spark.sql.Dataset
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import scala.reflect.runtime.universe
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import java.util.Calendar
import scala.io.Source
import java.sql.DriverManager
import java.sql.Connection
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.io.PrintWriter
import scala.compat.Platform.EOL

object read_hdfs {

  def main(args: Array[String]) {
    
	    val conf = new SparkConf().setAppName("pull")
			val sc = new SparkContext(conf)
			val sql = new org.apache.spark.sql.SQLContext(sc)

			import sql.implicits._

			val url_cci = "jdbc:oracle:thin:@50.112.133.49:1521:a11"
			val driver = "oracle.jdbc.driver.OracleDriver"
			val username_cci = "dbconnect"
			val password_cci = "c0lst0r"
			
			val url_tx = "jdbc:oracle:thin:@192.168.9.111:1521/A10"
			val username_tx = "datalake"
			val password_tx = "LOOP87fossil"
			
			val tableName = args(0)
			
			var cci_connection:Connection = null
			var tx_connection:Connection = null
			val write_conf = new Configuration()
	    write_conf.set("fs.defaultFS", "hdfs://nameservice1:8020")
	    val fs= FileSystem.get(write_conf)
      val output = fs.create(new Path("/tmp/mySample.txt"))
      val writer = new PrintWriter(output)
	    
			Class.forName(driver)
			
			cci_connection = DriverManager.getConnection(url_cci, username_cci, password_cci)
			tx_connection = DriverManager.getConnection(url_tx, username_tx, password_tx)
			  
			val statement_cci = cci_connection.createStatement()
		  val statement_tx = tx_connection.createStatement()
			
			for (TAB <- Source.fromFile(tableName).getLines){
			  

			  val cciCount = statement_cci.executeQuery("select count(*) from DELFOUR."+TAB)
			  val txCount = statement_tx.executeQuery("select count(*) from DELFOUR."+TAB)
			  
			     while ( cciCount.next() ) {
                  val host_cci = cciCount.getString(1)
		              println(host_cci)
		              //output.write("CCI Table "+TAB+" Count :"+host_cci.getBytes+EOL)
		               writer.write("CCI Table "+TAB+" Count :"+host_cci.getBytes+EOL)
           }
			  			     while ( txCount.next() ) {
                  val host_tx = txCount.getString(1)
                  println(host_tx)
		              //output.write("TX Table "+TAB+" Count :"+host_tx.getBytes+EOL)
                   writer.write("TX Table "+TAB+" Count :"+host_tx.getBytes+EOL)
		              
           }
			  
			}
			
	    cci_connection.close()
	    tx_connection.close()
						
  }


}