import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Features2 {
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

    val userViewCount = log
      .filter($"action_type" === 0)
      .groupBy("user_id")
      .count()
      .withColumnRenamed("count", "userViewCount")

    val userAddCount = log
      .filter($"action_type" === 1)
      .groupBy("user_id")
      .count()
      .withColumnRenamed("count", "userAddCount")

    val userBuyCount = log
      .filter($"action_type" === 2)
      .groupBy("user_id")
      .count()
      .withColumnRenamed("count", "userBuyCount")

    val userLikeCount = log
      .filter($"action_type" === 3)
      .groupBy("user_id")
      .count()
      .withColumnRenamed("count", "userLikeCount")

    val userViewItemNum = log
      .filter($"action_type" === 0)
      .groupBy("user_id")
      .agg(countDistinct("item_id"))
      .withColumnRenamed("count(DISTINCT item_id)", "userViewItemNum")

    val userAddItemNum = log
      .filter($"action_type" === 1)
      .groupBy("user_id")
      .agg(countDistinct("item_id"))
      .withColumnRenamed("count(DISTINCT item_id)", "userAddItemNum")

    val userBuyItemNum = log
      .filter($"action_type" === 2)
      .groupBy("user_id")
      .agg(countDistinct("item_id"))
      .withColumnRenamed("count(DISTINCT item_id)", "userBuyItemNum")

    val userLikeItemNum = log
      .filter($"action_type" === 3)
      .groupBy("user_id")
      .agg(countDistinct("item_id"))
      .withColumnRenamed("count(DISTINCT item_id)", "userLikeItemNum")

    train
      .select("user_id", "merchant_id")
      .join(userViewCount, Seq[String]("user_id"), "left")
      .join(userAddCount, Seq[String]("user_id"), "left")
      .join(userBuyCount, Seq[String]("user_id"), "left")
      .join(userLikeCount, Seq[String]("user_id"), "left")
      .join(userViewItemNum, Seq[String]("user_id"), "left")
      .join(userAddItemNum, Seq[String]("user_id"), "left")
      .join(userBuyItemNum, Seq[String]("user_id"), "left")
      .join(userLikeItemNum, Seq[String]("user_id"), "left")
      .write
      .format("csv")
      .mode("overwrite")
      .save("features_2")
  }
}
