import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import scala.math.BigDecimal

object firstdemo {
  val english = "english|en|eng"
  val spanish = "es|esp|spa|spanish|espanol"
  val turkish = "turkish|tr|tur|turc"
  val greek = "greek|el|ell"
  val italian = "italian|it|ita|italien"
  val all = (english :: spanish :: turkish :: greek :: italian :: Nil).mkString("|")

  def langIndep(s: String) = s.toLowerCase().replaceAll(all, "*")


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("wordCount")//.setMaster("local") //remove local
    val sc = new SparkContext(conf)
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "file:///tmp/spark-events")
    conf.set("spark.history.fs.logDirectory", "file:///tmp/spark-events")
    //conf.set("spark.driver.memory", "1k")
    //conf.set("spark.default.parallelism", "1")
    //conf.set("spark.executor.memory" , "1m")
//    tweak the following settings, set in terminal by `--driver-memory 1m` `--executor-memory 7g` for local cluster
//    or spark-submit --conf spark.executor.memory='4G'
//    spark.executor.cores                        6
//    spark.executor.memory                       8G
//    all configurations listed at https://spark.apache.org/docs/latest/configuration.html
//    # For the driver
//    spark.driver.cores                          6
//    spark.driver.memory                         8G

    val textFile = sc.textFile("/Users/xilaizhang/Desktop/Games.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.saveAsTextFile("/Users/xilaizhang/Desktop/output")


//    can also try spark.storage.memoryFraction
//    val logData = sc.textFile(logFile, 2).cache()
//    val numAs = logData.map(line => line.split(",")(0))
//    numAs.saveAsTextFile("file:///Users/xilaizhang/Desktop/downgrade/output")
  }
}
