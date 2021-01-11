import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Features1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val log = spark
      .read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("file:///usr/local/spark-2.4.0/input/user_log_format1.csv")

    val info = spark
      .read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("file:///usr/local/spark-2.4.0/input/user_info_format1.csv")

    val train = spark
      .read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("file:///usr/local/spark-2.4.0/input/train_format1.csv")

    val merchantViewCatNum = log
      .filter($"action_type" === 0)
      .groupBy("user_id", "merchant_id")
      .agg(countDistinct("cat_id"))
      .withColumnRenamed("count(DISTINCT cat_id)", "merchantViewCatNum")

    val merchantAddCatNum = log
      .filter($"action_type" === 1)
      .groupBy("user_id", "merchant_id")
      .agg(countDistinct("cat_id"))
      .withColumnRenamed("count(DISTINCT cat_id)", "merchantAddCatNum")

    val merchantBuyCatNum = log
      .filter($"action_type" === 2)
      .groupBy("user_id", "merchant_id")
      .agg(countDistinct("cat_id"))
      .withColumnRenamed("count(DISTINCT cat_id)", "merchantBuyCatNum")

    val merchantLikeCatNum = log
      .filter($"action_type" === 3)
      .groupBy("user_id", "merchant_id")
      .agg(countDistinct("cat_id"))
      .withColumnRenamed("count(DISTINCT cat_id)", "merchantLikeCatNum")

    val merchantViewCatCount = log
      .filter($"action_type" === 0)
      .groupBy("user_id", "merchant_id", "cat_id")
      .count()

    val merchantViewCatMost = merchantViewCatCount
      .join(merchantViewCatCount
        .groupBy("user_id", "merchant_id")
        .agg(max("count").alias("maxCount")),
        Seq[String]("user_id", "merchant_id"), "left")
      .filter($"count" === $"maxCount")
      .select("user_id", "merchant_id", "cat_id")
      .dropDuplicates("user_id", "merchant_id")
      .withColumnRenamed("cat_id", "merchantViewCatMost")

    val merchantAddCatCount = log
      .filter($"action_type" === 1)
      .groupBy("user_id", "merchant_id", "cat_id")
      .count()

    val merchantAddCatMost = merchantAddCatCount
      .join(merchantAddCatCount
        .groupBy("user_id", "merchant_id")
        .agg(max("count").alias("maxCount")),
        Seq[String]("user_id", "merchant_id"), "left")
      .filter($"count" === $"maxCount")
      .select("user_id", "merchant_id", "cat_id")
      .dropDuplicates("user_id", "merchant_id")
      .withColumnRenamed("cat_id", "merchantAddCatMost")

    val merchantBuyCatCount = log
      .filter($"action_type" === 2)
      .groupBy("user_id", "merchant_id", "cat_id")
      .count()

    val merchantBuyCatMost = merchantBuyCatCount
      .join(merchantBuyCatCount
        .groupBy("user_id", "merchant_id")
        .agg(max("count").alias("maxCount")),
        Seq[String]("user_id", "merchant_id"), "left")
      .filter($"count" === $"maxCount")
      .select("user_id", "merchant_id", "cat_id")
      .dropDuplicates("user_id", "merchant_id")
      .withColumnRenamed("cat_id", "merchantBuyCatMost")

    val merchantLikeCatCount = log
      .filter($"action_type" === 3)
      .groupBy("user_id", "merchant_id", "cat_id")
      .count()

    val merchantLikeCatMost = merchantLikeCatCount
      .join(merchantLikeCatCount
        .groupBy("user_id", "merchant_id")
        .agg(max("count").alias("maxCount")),
        Seq[String]("user_id", "merchant_id"), "left")
      .filter($"count" === $"maxCount")
      .select("user_id", "merchant_id", "cat_id")
      .dropDuplicates("user_id", "merchant_id")
      .withColumnRenamed("cat_id", "merchantLikeCatMost")

    val merchantActionDays = log
      .groupBy("user_id", "merchant_id")
      .agg(countDistinct("time_stamp"))
      .withColumnRenamed("count(DISTINCT time_stamp)", "merchantActionDays")

    train
      .select("user_id", "merchant_id")
      .join(merchantViewCatNum, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantAddCatNum, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantBuyCatNum, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantLikeCatNum, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantViewCatMost, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantAddCatMost, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantBuyCatMost, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantLikeCatMost, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantActionDays, Seq[String]("user_id", "merchant_id"), "left")
      .write
      .format("csv")
      .mode("overwrite")
      .save("features_1")
  }
}
