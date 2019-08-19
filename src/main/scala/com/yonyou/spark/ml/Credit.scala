package com.yonyou.spark.ml

import org.apache.spark._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorAssembler

import org.apache.spark.ml.tuning.{ ParamGridBuilder, CrossValidator }

import org.apache.spark.ml.{ Pipeline, PipelineStage }
import org.apache.spark.mllib.evaluation.RegressionMetrics
// 随机森林预测贷款风险
// 标签 -> 是否可信：0或者1
// 特征 -> {“1存款”，“2期限”，“3历史记录”，“4目的”，“5数额”，
//          “6储蓄”，“7是否在职”，“8婚姻”，“9担保人”，“10居住时间”，“11资产”，
//         “12年龄”，“13历史信用”，“14居住公寓”，“15贷款”，“16职业”，“17监护人”，“18是否有电话”，“19外籍”}
object Credit {

  case class Credit(
    creditability: Double,
    balance: Double, duration: Double, history: Double, purpose: Double, amount: Double,
    savings: Double, employment: Double, instPercent: Double, sexMarried: Double, guarantors: Double,
    residenceDuration: Double, assets: Double, age: Double, concCredit: Double, apartment: Double,
    credits: Double, occupation: Double, dependents: Double, hasPhone: Double, foreign: Double
  )

  val cols : List[String] = List("creditability","balance","duration","history","purpose","amount",
  "savings","employment","instPercent","sexMarried","guarantors",
  "residenceDuration","assets","age","concCredit","apartment",
  "credits","occupation","dependents","hasPhone","foreign")

  def parseCredit(line: Array[Double]): Credit = {
    Credit(
      line(0),
      line(1) - 1, line(2), line(3), line(4), line(5),
      line(6) - 1, line(7) - 1, line(8), line(9) - 1, line(10) - 1,
      line(11) - 1, line(12) - 1, line(13), line(14) - 1, line(15) - 1,
      line(16) - 1, line(17) - 1, line(18) - 1, line(19) - 1, line(20) - 1
    )
  }

  def parseRDD(rdd: RDD[String]): RDD[Array[Double]] = {
    rdd.map(_.split(",")).map(_.map(_.toDouble))
  }

  def main(args: Array[String]) {
//    1.6写法
    val conf = new SparkConf().setAppName("SparkDFebay").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext._
    import sqlContext.implicits._

    val creditDF = parseRDD(sc.textFile("data/germancredit.csv")).map(parseCredit).toDF().cache()
    creditDF.createOrReplaceTempView("credit")
    creditDF.printSchema

    creditDF.show

    // 2.0 写法 enableHiveSupport()
    //val spark = SparkSession.builder().appName("SparkDFebay").master("local[*]").getOrCreate()
    //spark.read.option("header","true").csv("src/main/resources/sales.csv")
    //get all settings
    //val configMap:Map[String, String] = spark.conf.getAll
    //val creditDF = spark.read.csv("data/germancredit.csv").toDF(cols:_*)
    //creditDF.createOrReplaceTempView("credit")

    // 2.0 end

    sqlContext.sql("SELECT creditability, avg(balance) as avgbalance, avg(amount) as avgamt, avg(duration) as avgdur  FROM credit GROUP BY creditability ").show

    creditDF.describe("balance").show
    creditDF.groupBy("creditability").avg("balance").show

    val featureCols = Array("balance", "duration", "history", "purpose", "amount",
      "savings", "employment", "instPercent", "sexMarried", "guarantors",
      "residenceDuration", "assets", "age", "concCredit", "apartment",
      "credits", "occupation", "dependents", "hasPhone", "foreign")
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val df2 = assembler.transform(creditDF)
    df2.show
    // http://www.2cto.com/net/201609/544953.html
    val labelIndexer = new StringIndexer().setInputCol("creditability").setOutputCol("label")//规范化
    val df3 = labelIndexer.fit(df2).transform(df2)
    df3.show
    val splitSeed = 5043
    val Array(trainingData, testData) = df3.randomSplit(Array(0.7, 0.3), splitSeed)
    /*maxDepth：每棵树的最大深度。增加树的深度可以提高模型的效果，但是会延长训练时间。
    maxBins：连续特征离散化时选用的最大分桶个数，并且决定每个节点如何分裂。
    impurity：计算信息增益的指标
    auto：在每个节点分裂时是否自动选择参与的特征个数
    seed：随机数生成种子*/
    val classifier = new RandomForestClassifier().setImpurity("gini").setMaxDepth(3).setNumTrees(20).setFeatureSubsetStrategy("auto").setSeed(5043)
    val model = classifier.fit(trainingData)

    // test pipeline
//    val pipeline1 = new Pipeline().setStages(Array(assembler, labelIndexer, classifier))
//    val randomForestModel = pipeline1.fit(trainingData)
//    randomForestModel.transform(testData).select("label","features").show
    // test pipeline end

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label")
    val predictions = model.transform(testData)
    model.toDebugString

    // 打印出一些测试结果
    val accuracy = evaluator.evaluate(predictions)
    println("accuracy before pipeline fitting" + accuracy)

    val rm = new RegressionMetrics(
      predictions.select("prediction", "label").rdd.map(x =>
        (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
    )
    println("MSE: " + rm.meanSquaredError)//均方误差
    println("MAE: " + rm.meanAbsoluteError)//平均绝对误差
    println("RMSE Squared: " + rm.rootMeanSquaredError)
    println("R Squared: " + rm.r2)
    println("Explained Variance: " + rm.explainedVariance + "\n")

//    val paramGrid = new ParamGridBuilder()
//      .addGrid(classifier.maxBins, Array(25, 31))
//      .addGrid(classifier.maxDepth, Array(5, 10))
//      .addGrid(classifier.numTrees, Array(20, 60))
//      .addGrid(classifier.impurity, Array("entropy", "gini"))
//      .build()
//
//    val steps: Array[PipelineStage] = Array(classifier)
//    val pipeline = new Pipeline().setStages(steps)
//
//    val cv = new CrossValidator()
//      .setEstimator(pipeline)
//      .setEvaluator(evaluator)
//      .setEstimatorParamMaps(paramGrid)
//      .setNumFolds(2)
//
//    val pipelineFittedModel = cv.fit(trainingData)
//
//    val predictions2 = pipelineFittedModel.transform(testData)
//    val accuracy2 = evaluator.evaluate(predictions2)
//    println("accuracy after pipeline fitting" + accuracy2)
//
//    println(pipelineFittedModel.bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel].stages(0))
//
//    pipelineFittedModel
//      .bestModel.asInstanceOf[org.apache.spark.ml.PipelineModel]
//      .stages(0)
//      .extractParamMap
//
//    val rm2 = new RegressionMetrics(
//      predictions2.select("prediction", "label").rdd.map(x =>
//        (x(0).asInstanceOf[Double], x(1).asInstanceOf[Double]))
//    )
//
//    println("MSE: " + rm2.meanSquaredError)
//    println("MAE: " + rm2.meanAbsoluteError)
//    println("RMSE Squared: " + rm2.rootMeanSquaredError)
//    println("R Squared: " + rm2.r2)
//    println("Explained Variance: " + rm2.explainedVariance + "\n")

  }
}

