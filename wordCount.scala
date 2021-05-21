import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
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
  def func1(s: String): (Int, Array[String]) = {
    val words = s.split(" ")
    return Tuple2(words(0).toInt % 4, words.slice(1, words.size))
  }
  def func2(v: (Int, Array[String])): (Int, String) = {
    return Tuple2(v._1, v._2.mkString(" "))
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("my1gb")//.setMaster("local") //remove local
    val sc = new SparkContext(conf)
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", "file:///tmp/spark-events")
    conf.set("spark.history.fs.logDirectory", "file:///tmp/spark-events")
    //conf.set("spark.driver.memory", "600m")
    //conf.set("spark.default.parallelism", "1")
    //conf.set("spark.executor.memory" , "1g")
//    tweak the following settings, set in terminal by `--driver-memory 1m` `--executor-memory 7g` for local cluster
//    or spark-submit --conf spark.executor.memory='4G'
//    spark.executor.cores                        6
//    spark.executor.memory                       8G
//    all configurations listed at https://spark.apache.org/docs/latest/configuration.html
//    # For the driver
//    spark.driver.cores                          6
//    spark.driver.memory                         8G

    val textFile = sc.textFile("/Users/xilaizhang/Desktop/partition.txt")
    val counts = textFile.map(line => func1(line))
    //println("outputing counts" + counts)
      .partitionBy(new HashPartitioner(4))
      .reduceByKey(_ ++ _)
        .map(v=> (
          v._1, v._2.mkString(" ")
        ))

    counts.saveAsTextFile("/Users/xilaizhang/Desktop/output")


//    can also try spark.storage.memoryFraction
//    val logData = sc.textFile(logFile, 2).cache()
//    val numAs = logData.map(line => line.split(",")(0))
//    numAs.saveAsTextFile("file:///Users/xilaizhang/Desktop/downgrade/output")
  }
}
