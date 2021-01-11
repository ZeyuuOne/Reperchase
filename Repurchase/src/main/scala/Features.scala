import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Features {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val features0 = spark
      .read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("file:///usr/local/spark-2.4.0/features_0.csv")

    val features1 = spark
      .read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("file:///usr/local/spark-2.4.0/features_1.csv")

    val features2 = spark
      .read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("file:///usr/local/spark-2.4.0/features_2.csv")

    val features3 = spark
      .read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("file:///usr/local/spark-2.4.0/features_3.csv")

    features0
      .join(features1, Seq[String]("user_id", "merchant_id"), "left")
      .join(features2, Seq[String]("user_id", "merchant_id"), "left")
      .join(features3, Seq[String]("user_id", "merchant_id"), "left")
      .write
      .format("csv")
      .mode("overwrite")
      .save("features")
  }
}