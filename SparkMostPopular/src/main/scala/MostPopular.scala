import org.apache.spark.{SparkConf, SparkContext}

object MostPopular {
  def main(args: Array[String]): Unit = {
    val inputFile = "file:///usr/local/spark-2.4.0/input/user_log_format1.csv"
    val infoFile = "file:///usr/local/spark-2.4.0/input/user_info_format1.csv"
    val conf = new SparkConf().setAppName("Most Popular Item");
    val sc = new SparkContext(conf)
    val info = sc.textFile(infoFile).cache()
    val userU30 = info
      .map(line => line.split(","))
      .filter(line => line.size >= 2 && !line(0).equals("user_id") && (line(1).equals("1") || line(1).equals("2") || line(1).equals("3")))
      .map(line => line(0))
      .collect()
      .toSet
    val input = sc.textFile(inputFile).cache()
    val itemTop100 = input
      .map(line => line.split(","))
      .filter(line => !line(0).equals("user_id") && line(5).equals("1111") && !line(6).equals("0"))
      .map(line => (line(1), 1))
      .reduceByKey { case (x, y) => x + y }
      .sortBy(_._2,false)
      .take(100)
    val merchantTop100 = input
      .map(line => line.split(","))
      .filter(line => !line(0).equals("user_id") && line(5).equals("1111") && !line(6).equals("0") && userU30.contains(line(0)))
      .map(line => (line(3), 1))
      .reduceByKey { case (x, y) => x + y }
      .sortBy(_._2,false)
      .take(100)
    sc.parallelize(itemTop100).saveAsTextFile("temp/output_0")
    sc.parallelize(merchantTop100).saveAsTextFile("temp/output_1")
  }
}