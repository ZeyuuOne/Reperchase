import org.apache.spark.sql.{SaveMode, SparkSession}

object BuyerInfo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Buyer Info")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    import spark.implicits._
    val inputDF = spark
      .read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("file:///usr/local/spark-2.4.0/input/user_log_format1.csv")
    val infoDF = spark
      .read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("file:///usr/local/spark-2.4.0/input/user_info_format1.csv")
    val buyer1111 = inputDF
      .filter($"time_stamp" === 1111 && $"action_type" === 2)
      .select("user_id")
      .distinct()
      .join(infoDF, "user_id")
    buyer1111
      .groupBy("age_range")
      .count()
      .write
      .format("csv")
      .mode("overwrite")
      .save("output_0")
    buyer1111
      .groupBy("gender")
      .count()
      .write
      .format("csv")
      .mode("overwrite")
      .save("output_1")
  }
}