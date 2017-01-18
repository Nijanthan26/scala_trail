
import org.apache.spark.SparkContext, org.apache.spark.SparkConf

object hdfs {
    def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("scala spark").setMaster("local")
    val sc = new SparkContext(conf)
    val i = List(1, 2, 3, 4, 5)
    val dataRDD = sc.parallelize(i)
    dataRDD.saveAsTextFile(args(0))
  }
}