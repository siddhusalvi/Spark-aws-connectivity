import org.apache.spark.{SparkConf, SparkContext}

object Wordcount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("s3wordcounter").setMaster("local")
    val sc = new SparkContext(conf)

    val bucket = "sparkweek6"
    val file = "words.csv"

	//added environment variable and other data
    val key = System.getenv("key")
    val secretkey = System.getenv("secretkey")

    System.setProperty("com.amazonaws.services.s3.enableV4", "true")
    sc.hadoopConfiguration.set("fs.s3a.access.key", key)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", secretkey)
    sc.hadoopConfiguration.set("fs.s3a.endpoint", "s3.us-east-2.amazonaws.com")
    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    val text = sc.textFile("s3a://" + bucket + "/" + file)
    val words = text.flatMap(line => line.split(","))
    val pairs = words.map(word => (word, 1))
    val counts = pairs.reduceByKey(_ + _).sortByKey()
    counts.foreach(println(_))

  }
}
