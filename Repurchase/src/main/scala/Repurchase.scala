import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

object Repurchase {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("Spark SQL")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    import spark.implicits._

    val featuresTrain = spark
      .read.format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("file:///usr/local/spark-2.4.0/features.csv")

    val assembler = new VectorAssembler()
      .setInputCols(Array(
        "age_range", "gender", "merchantViewCount", "merchantAddCount", "merchantBuyCount", "merchantLikeCount",
        "merchantViewItemNum", "merchantAddItemNum", "merchantBuyItemNum", "merchantLikeItemNum", "merchantViewCatNum",
        "merchantAddCatNum", "merchantBuyCatNum", "merchantLikeCatNum", "merchantViewCatMost", "merchantAddCatMost",
        "merchantBuyCatMost", "merchantLikeCatMost", "merchantActionDays", "userViewCount", "userAddCount",
        "userBuyCount", "userLikeCount", "userViewItemNum", "userAddItemNum", "userBuyItemNum", "userLikeItemNum",
        "userViewCatNum", "userAddCatNum", "userBuyCatNum", "userLikeCatNum", "userViewCatMost", "userAddCatMost",
        "userBuyCatMost", "userLikeCatMost", "userActionDays"))
      .setOutputCol("features")

    val assemblerTrain = assembler.transform(featuresTrain)
    val Array(train, test) = assemblerTrain.randomSplit(Array(0.8, 0.2))

    val rfc = new RandomForestClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")

    val rfcModel = rfc.fit(train)
    val trainPredict = rfcModel.transform(train)
    val testPredict = rfcModel.transform(test)
    val auc = new BinaryClassificationEvaluator()
      .setMetricName("areaUnderROC")
      .evaluate(testPredict)
    println()
    print("----- TEST AUC: ")
    print(auc)
    println(" -----")
    println()
  }
}
