package com.bigdata.spark.ml

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.tuning.{ParamGridBuilder, TrainValidationSplit}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType

/**
  *
  * 在Spark2.1用PipeLine的方式编写的线性回归
  * Created by zhangqs on 2017/3/31.
  */
class LinearRegressionTest {
  def train(sparkSession: SparkSession,url:String): Unit = {
    val spark = sparkSession

    val cols : List[String] = List("instant", "dteday", "season", "yr", "mnth", "hr", "holiday", "weekday", "workingday", "weathersit", "temp", "atemp", "hum", "windspeed", "casual", "registered", "cnt")

    //将数据文件读成DataFrame
    var df = spark.read.csv(url).toDF(cols:_*)

    //将因变量列转换为Double类型
    df = df.withColumn("pre",df("cnt").cast(DoubleType))
    df = df.drop("cnt").withColumnRenamed("pre","cnt")

    //选取自变量列
    val features = cols.slice(2,10)

    val featureTransformer : Array[StringIndexer] = new Array[StringIndexer](features.length);

    val featureTra : Array[String] = new Array[String](features.length)
    //将Nominal的的自变量字段用Index编码
    var index = 0
    features.foreach(col => {
      val traName = col + "Tran"
      val stringIndexer = new StringIndexer().setInputCol(col).setOutputCol(traName)
      featureTransformer(index) = stringIndexer
      featureTra(index) = traName
      index += 1
    })
    featureTransformer(0).fit(df)

    //指定自变量列
    val featureVector = new VectorAssembler().setInputCols(featureTra).setOutputCol("features")

    //申明模型
    val linear = new LinearRegression().setFeaturesCol("features").setLabelCol("cnt")

    //随机切分训练机和测试集
    val Array(trainingDF, testDF) = df.randomSplit(Array(0.8, 0.2))

    //设置管道
    val pipeline = new Pipeline().setStages(featureTransformer ++ Array(featureVector,linear))
    // 建立参数网格  elasticNetParam=正则化参数
    val paramGrid = new ParamGridBuilder().addGrid(linear.fitIntercept).addGrid(linear.elasticNetParam, Array(0.0, 0.5, 1.0)).addGrid(linear.maxIter, Array(10, 100)).build()

    // 选择(prediction, true label)，计算测试误差。
    // 注意RegEvaluator.isLargerBetter，评估的度量值是大的好，还是小的好，系统会自动识别
    val RegEvaluator = new RegressionEvaluator().setLabelCol(linear.getLabelCol).setPredictionCol(linear.getPredictionCol).setMetricName("rmse")

    // 与CrossValidator不同的 只评估每一种参数组合一次。而不是像CrossValidator评估k次，TrainValidationSplit 只有一次。因此不是很昂贵，但是如果训练数据集不够大就不能产生能信赖的结果。
    val trainValidationSplit = new TrainValidationSplit().setEstimator(pipeline).setEvaluator(RegEvaluator).setEstimatorParamMaps(paramGrid).setTrainRatio(0.8) // 数据分割比例

    // Run train validation split, and choose the best set of parameters.
    val tvModel = trainValidationSplit.fit(trainingDF)

    // 查看模型全部参数
    tvModel.extractParamMap()

    tvModel.getEstimatorParamMaps.length
    tvModel.getEstimatorParamMaps.foreach {
      println
    } // 参数组合的集合

    tvModel.getEvaluator.extractParamMap() // 评估的参数

    tvModel.getEvaluator.isLargerBetter // 评估的度量值是大的好，还是小的好

    tvModel.getTrainRatio

    // 用最好的参数组合，做出预测
    tvModel.transform(testDF).select("features", "cnt", "prediction").show()

    //
  }

}

object LinearRegressionTest{

  def main(args: Array[String]) {

    //    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local[*]")
    //    val sc = new SparkContext(conf)

    // .config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport() .getOrCreate()
    val sparkSession = SparkSession.builder().master("local[*]").appName("SparkDFebay").getOrCreate()

    sparkSession.sql("")
    val linearRegTest = new LinearRegressionTest()
    linearRegTest.train(sparkSession,"data/hour_noheader.csv")
  }
}
