import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object fetchs3file extends App {

  val spark = SparkSession.builder().appName("wordcounter").master("local[*]").getOrCreate()
  val sc = spark.sparkContext

  val bucket = "sparkweek6"
  val file = "words.csv"

  val key =
  val secretkey =

  System.setProperty("com.amazonaws.services.s3.enableV4", "true")
  sc.hadoopConfiguration.set("fs.s3a.access.key",key)
  sc.hadoopConfiguration.set("fs.s3a.secret.key",secretkey)
  sc.hadoopConfiguration.set("fs.s3a.endpoint","s3.us-east-2.amazonaws.com")
  sc.hadoopConfiguration.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")


  val data = sc.textFile("s3a://"+bucket+"/"+file)
  data.foreach(println(_))

}
