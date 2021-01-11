import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Features3 {
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

    val userViewCatNum = log
      .filter($"action_type" === 0)
      .groupBy("user_id")
      .agg(countDistinct("cat_id"))
      .withColumnRenamed("count(DISTINCT cat_id)", "userViewCatNum")

    val userAddCatNum = log
      .filter($"action_type" === 1)
      .groupBy("user_id")
      .agg(countDistinct("cat_id"))
      .withColumnRenamed("count(DISTINCT cat_id)", "userAddCatNum")

    val userBuyCatNum = log
      .filter($"action_type" === 2)
      .groupBy("user_id")
      .agg(countDistinct("cat_id"))
      .withColumnRenamed("count(DISTINCT cat_id)", "userBuyCatNum")

    val userLikeCatNum = log
      .filter($"action_type" === 3)
      .groupBy("user_id")
      .agg(countDistinct("cat_id"))
      .withColumnRenamed("count(DISTINCT cat_id)", "userLikeCatNum")

    val userViewCatCount = log
      .filter($"action_type" === 0)
      .groupBy("user_id", "cat_id")
      .count()

    val userViewCatMost = userViewCatCount
      .join(userViewCatCount
        .groupBy("user_id")
        .agg(max("count").alias("maxCount")),
        Seq[String]("user_id"), "left")
      .filter($"count" === $"maxCount")
      .select("user_id", "cat_id")
      .dropDuplicates("user_id")
      .withColumnRenamed("cat_id", "userViewCatMost")

    val userAddCatCount = log
      .filter($"action_type" === 1)
      .groupBy("user_id", "cat_id")
      .count()

    val userAddCatMost = userAddCatCount
      .join(userAddCatCount
        .groupBy("user_id")
        .agg(max("count").alias("maxCount")),
        Seq[String]("user_id"), "left")
      .filter($"count" === $"maxCount")
      .select("user_id", "cat_id")
      .dropDuplicates("user_id")
      .withColumnRenamed("cat_id", "userAddCatMost")

    val userBuyCatCount = log
      .filter($"action_type" === 2)
      .groupBy("user_id", "cat_id")
      .count()

    val userBuyCatMost = userBuyCatCount
      .join(userBuyCatCount
        .groupBy("user_id")
        .agg(max("count").alias("maxCount")),
        Seq[String]("user_id"), "left")
      .filter($"count" === $"maxCount")
      .select("user_id", "cat_id")
      .dropDuplicates("user_id")
      .withColumnRenamed("cat_id", "userBuyCatMost")

    val userLikeCatCount = log
      .filter($"action_type" === 3)
      .groupBy("user_id", "cat_id")
      .count()

    val userLikeCatMost = userLikeCatCount
      .join(userLikeCatCount
        .groupBy("user_id")
        .agg(max("count").alias("maxCount")),
        Seq[String]("user_id"), "left")
      .filter($"count" === $"maxCount")
      .select("user_id", "cat_id")
      .dropDuplicates("user_id")
      .withColumnRenamed("cat_id", "userLikeCatMost")

    val userActionDays = log
      .groupBy("user_id")
      .agg(countDistinct("time_stamp"))
      .withColumnRenamed("count(DISTINCT time_stamp)", "userActionDays")

    train
      .select("user_id", "merchant_id")
      .join(userViewCatNum, Seq[String]("user_id"), "left")
      .join(userAddCatNum, Seq[String]("user_id"), "left")
      .join(userBuyCatNum, Seq[String]("user_id"), "left")
      .join(userLikeCatNum, Seq[String]("user_id"), "left")
      .join(userViewCatMost, Seq[String]("user_id"), "left")
      .join(userAddCatMost, Seq[String]("user_id"), "left")
      .join(userBuyCatMost, Seq[String]("user_id"), "left")
      .join(userLikeCatMost, Seq[String]("user_id"), "left")
      .join(userActionDays, Seq[String]("user_id"), "left")
      .join(train, Seq[String]("user_id", "merchant_id"), "left")
      .write
      .format("csv")
      .mode("overwrite")
      .save("features_3")
  }
}