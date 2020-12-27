import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object BuyerInfo {
  def main(args: Array[String]): Unit = {
    val inputFile = "file:///usr/local/spark-2.4.0/input/user_log_format1.csv"
    val infoFile = "file:///usr/local/spark-2.4.0/input/user_info_format1.csv"
    val conf = new SparkConf().setAppName("User Info")
    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile).cache()
    val info = sc.textFile(infoFile).cache()
    val buyer = input
      .map(line => line.split(","))
      .filter(line => line(5).equals("1111") && line(6).equals("2"))
      .map(line => line(0))
      .collect()
      .toSet
    val gender = info
      .map(line => line.split(","))
      .filter(line => buyer.contains(line(0)))
      .map(line => if (line.size < 3) Array[String](line(0), "0", "2") else line)
      .map(line => (line(2), 1))
      .reduceByKey { case (x, y) => x + y }
      .sortBy(x => x._2, false)
    val buyerSum = gender
      .map(line => line._2)
      .reduce(_ + _)
    val genderProp = gender
      .map(line => (line._1, line._2.toDouble/buyerSum.toDouble))
    val age = info
      .map(line => line.split(","))
      .filter(line => buyer.contains(line(0)))
      .map(line => if (line.size < 2 || (line.size >= 2 && line(1).equals(""))) Array[String](line(0), "0") else line)
      .map(line => (line(1), 1))
      .reduceByKey { case (x, y) => x + y }
      .sortBy(x => x._2, false)
    val ageProp = age
      .map(line => (line._1, line._2.toDouble/buyerSum.toDouble))
    genderProp.saveAsTextFile("temp/output_0")
    ageProp.saveAsTextFile("temp/output_1")
  }
}