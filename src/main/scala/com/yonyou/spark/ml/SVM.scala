package com.yonyou.spark.ml

import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.SparkSession

/**
  * Created by zhangqs on 2017/3/28.
  */
class SVM {

  //训练SVM模型
  def train(spark: SparkSession, url:String):Double ={
    //读入指定格式的数据
//    val data = MLUtils.loadLibSVMFile(sparkContext,url)

    //将 list 转换为Rdd
//    sparkContext.parallelize(list)

    //读入数据并处理成LabelPoint 的Rdd
    val records =  spark.sparkContext.textFile(url).map(_.split("\t"));
    //sparkContext.textFile(url).map(_.split("\t"));
    //转换为LabeledPoint的RDD对象
    //val data1 = spark.read.text(url).as[String]
   // val words = data1.flatMap(value=>value.split("\\s+"))

    val data = records.map(row => {
      val trimmed = row.map(_.replaceAll("\"",""))
      val label = trimmed(row.size - 1).toInt
      val feature = trimmed.slice(4,row.size - 1).map(d => if(d == "?") 0.0 else d.toDouble)
      LabeledPoint(label,Vectors.dense(feature))
    })

    //切分为训练数据集和测试集
    val Array(trainSet, testSet) = data.randomSplit(Array(0.7, 0.3), 1L)

    val model = SVMWithSGD.train(data, 10)

    model.clearThreshold()

    val scoreAndLabels = testSet.map { point =>
      //预测分类
      val score = model.predict(point.features)
      //实际分类
      (score, point.label)
    }

    //ROC评价分类模型
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    val auROC = metrics.areaUnderROC()

    //ROC相关理论
    // http://blog.csdn.net/abcjennifer/article/details/7359370

    return auROC
  }

}

object SVM{
  def main(args : Array[String]): Unit ={
    val spark = SparkSession.builder().appName("SVM test").master("local[*]").getOrCreate()
    val svmTest = new SVM()
    println(svmTest.train(spark, "data/svm_noheader.csv"))
  }
}
