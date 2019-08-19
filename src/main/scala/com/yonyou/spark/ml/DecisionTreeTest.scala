package com.yonyou.spark.ml

import org.apache.spark.ml.classification.{DecisionTreeClassificationModel, RandomForestClassificationModel}
import org.apache.spark.ml.tree.DecisionTreeModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 决策树分类
  */
object DecisionTreeTest {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("DecisionTree").setMaster("local")
    val sc = new SparkContext(conf)

    //训练数据
    val data1 = sc.textFile("data/Tree1.txt")

    //测试数据
    val data2 = sc.textFile("data/Tree2.txt")


    //转换成向量
    val tree1 = data1.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    val tree2 = data2.map { line =>
      val parts = line.split(',')
      LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
    }

    //赋值
    val (trainingData, testData) = (tree1, tree2)

    //分类
    val numClasses = 2
    //参数categoricalFeaturesInfo是一个映射表，用来指明哪些特征是分类的，以及他们有多少个类。比如，特征１是一个标签为１，０的二元特征，
    // 特征２是０，１，２的三元特征，则传递{1: 2, 2: 3}。如果没有特征是分类的，数据是连续变量，那么我们可以传递空表。
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"//基尼不纯度    impurity表示结点的不纯净度测量，分类问题采用 gini或者entropy，而回归必须用 variance。

    //最大深度
    val maxDepth = 5
    //最大分支
    val maxBins = 32

    //模型训练
    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    model.save(sc, "data/model/DecisonTreeXiangQinModel")

    //val model = DecisionTreeClassificationModel.load("data/model/DecisonTreeXiangQinModel")
    //val model = DecisionTreeModel.load(sc, "data/model/DecisonTreeXiangQinModel");

    //模型预测
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    //测试值与真实值对比
    val print_predict = labelAndPreds.take(15)
    println("label" + "\t" + "prediction")
    for (i <- 0 to print_predict.length - 1) {
      println(print_predict(i)._1 + "\t" + print_predict(i)._2)
    }

    //树的错误率
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    //打印树的判断值
    println("Learned classification tree model:\n" + model.toDebugString)
    println("==============")
    //println("Learned classification tree model:\n" + sameModel.toDebugString)

  }

}
