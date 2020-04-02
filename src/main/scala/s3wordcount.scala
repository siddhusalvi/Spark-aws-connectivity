import org.apache.spark.{SparkConf, SparkContext}
object s3wordcount extends App {
  val conf = new SparkConf().setAppName("s3wordcounter").setMaster("local")
  val sc = new SparkContext(conf)

  val bucket = "sparkweek6"
  val file = "words.csv"

  val key = ""
  val secretkey = ""

  System.setProperty("com.amazonaws.services.s3.enableV4", "true")
  sc.hadoopConfiguration.set("fs.s3a.access.key",key)
  sc.hadoopConfiguration.set("fs.s3a.secret.key",secretkey)
  sc.hadoopConfiguration.set("fs.s3a.endpoint","s3.us-east-2.amazonaws.com")
  sc.hadoopConfiguration.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")

  val text = sc.textFile("s3a://"+bucket+"/"+file)
  val words = text.flatMap(line => line.split(","))
  val pairs = words.map(word =>(word,1))
  val counts = pairs.reduceByKey(_+_).sortByKey()
  counts.foreach(println(_))

}
