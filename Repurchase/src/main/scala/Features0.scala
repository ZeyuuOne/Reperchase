import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Features0 {
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

    val merchantViewCount = log
      .filter($"action_type" === 0)
      .groupBy("user_id", "merchant_id")
      .count()
      .withColumnRenamed("count", "merchantViewCount")

    val merchantAddCount = log
      .filter($"action_type" === 1)
      .groupBy("user_id", "merchant_id")
      .count()
      .withColumnRenamed("count", "merchantAddCount")

    val merchantBuyCount = log
      .filter($"action_type" === 2)
      .groupBy("user_id", "merchant_id")
      .count()
      .withColumnRenamed("count", "merchantBuyCount")

    val merchantLikeCount = log
      .filter($"action_type" === 3)
      .groupBy("user_id", "merchant_id")
      .count()
      .withColumnRenamed("count", "merchantLikeCount")

    val merchantViewItemNum = log
      .filter($"action_type" === 0)
      .groupBy("user_id", "merchant_id")
      .agg(countDistinct("item_id"))
      .withColumnRenamed("count(DISTINCT item_id)", "merchantViewItemNum")

    val merchantAddItemNum = log
      .filter($"action_type" === 1)
      .groupBy("user_id", "merchant_id")
      .agg(countDistinct("item_id"))
      .withColumnRenamed("count(DISTINCT item_id)", "merchantAddItemNum")

    val merchantBuyItemNum = log
      .filter($"action_type" === 2)
      .groupBy("user_id", "merchant_id")
      .agg(countDistinct("item_id"))
      .withColumnRenamed("count(DISTINCT item_id)", "merchantBuyItemNum")

    val merchantLikeItemNum = log
      .filter($"action_type" === 3)
      .groupBy("user_id", "merchant_id")
      .agg(countDistinct("item_id"))
      .withColumnRenamed("count(DISTINCT item_id)", "merchantLikeItemNum")

    train
      .select("user_id", "merchant_id")
      .join(info, Seq[String]("user_id"), "left")
      .join(merchantViewCount, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantAddCount, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantBuyCount, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantLikeCount, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantViewItemNum, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantAddItemNum, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantBuyItemNum, Seq[String]("user_id", "merchant_id"), "left")
      .join(merchantLikeItemNum, Seq[String]("user_id", "merchant_id"), "left")
      .write
      .format("csv")
      .mode("overwrite")
      .save("features_0")
  }
}
