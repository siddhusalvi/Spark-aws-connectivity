import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object s3wordcounter extends App {
  val spark = SparkSession.builder().appName("wordcounter").master("local[*]").getOrCreate()
  val sc = spark.sparkContext
  val key =
  val secretkey =
  val bucket = "sparkweek6"
  val file = "words.csv"

  sc.hadoopConfiguration.set("fs.s3a.access.key",key)
  sc.hadoopConfiguration.set("fs.s3a.secret.key",secretkey)
  sc.hadoopConfiguration.set("fs.s3a.endpoint","s3.us-east-2.amazonaws.com")
  sc.hadoopConfiguration.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

//
//  val data = sc.textFile("s3a://"+bucket+"/"+"data.txt")
//  data.collect()


  val data = sc.textFile("C:\\Users\\Siddesh\\Downloads\\data.txt")
  for(line <- data){println(line)}





}