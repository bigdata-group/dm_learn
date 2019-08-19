package com.yonyou.spark.example
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
  * 使用Scala开发本地测试的Spark WordCount程序
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    /**
      * 第一步:创建Spark的配置对象SparkConf，设置Spark程序的运行时的配置信息
      * 例如说通过setMaster来设置程序要连接的Spark集群的Master的URL
      * 如果设置为local，则代表Spark程序在本地运行，特别适合于配置条件的较差的人
      *
      */

    //val conf = new SparkConf().setMaster("spark://192.168.2.128:7077").setAppName("word-count")
    val conf = new SparkConf().setMaster("local").setAppName("word-count")

    /**
      * 第二步:创建SparkContext对象
      * SparkContext是Spark程序所有功能的唯一入口，无论是采用Scala，Java，Python等都必须有一个SparkContext
      * SparkContext核心作用：初始化Spark应用程序运行所需要的核心组件，包括DAGScheduler，TaskScheduler，Scheduler
      * 同时还会负责Spark程序往Master注册程序等
      * SparkContext是整个Spark应用程序中最为至关重要的一个对象。
      */

    val sc = new SparkContext(conf)     //创建SparkContext对象，通过传入SparkConf实例来定制Spark运行的具体参数和配置信息

    /**
      * 第三步:根据具体的数据来源（HDFS，HBase，Local FS（本地文件系统） ，DB，S3（云上）等）通过SparkContext来创建RDD
      * RDD的创建基本有三种方式，根据外部的数据来源（例如HDFS），根据Scala集合，由其他的RDD操作产生
      * 数据会被RDD划分成为一系列的Partitions，分配到每个Partition的数据属于一个Task的处理范畴
      */

    //文件的路径，最小并行度（根据机器数量来决定）
    //val lines:RDD[String]= sc.textFile("F://spark//spark-1.6.2-bin-hadoop2.6//README.md", 1)    //读取本地文件，并设置Partition = 1
    val lines= sc.textFile("E:\\workspace-bigdata02\\dm_learn\\data\\1.txt", 1)    //读取本地文件，并设置Partition = 1   //类型推导得出lines为RDD
    /**
      * 第四步:对初始的RDD进行Transformation级别的处理，例如map，filter等高阶函数等的编程，来进行具体的数据计算
      *    4.1:将每一行的字符串拆分成单个的单词
      *    4.2:在单词拆分的基础上对每个单词的实例计数为1，也就是word =>(word,1)
      *    4.3:在每个单词实例计数为1基础之上统计每个单词在文件出现的总次数
      */

    //对每一行的字符串进行单词的拆分并把所有行的拆分结果通过flat合并成为一个大的单词集合
    val words = lines.flatMap { line => line.split(" ") }    //words同样是RDD类型
    val pairs = words.map { word => (word,1) }
    val wordCounts = pairs.reduceByKey(_+_)       //对相同的key，进行value的累加（包括Local和Reducer级别同时Reduce）


    wordCounts.foreach(word => println(word._1 + " : " + word._2))

    /*
    * for 循环中的 yield 会把当前的元素记下来，保存在集合中，循环结束后将返回该集合。Scala 中 for 循环是有返回值的。
    * 如果被循环的是 Map，返回的就是  Map，被循环的是 List，返回的就是 List
    *
    * */
    lines.map(_.split(";")).flatMap(x=>{
      for(i<-0 until x.length-1) yield (x(i)+","+x(i+1),1)
    }).reduceByKey(_+_).foreach(println)

    sc.stop()    //注意一定要将SparkContext的对象停止，因为SparkContext运行时会创建很多的对象


    /*这个程序运行之后一定会有一个错误，因为 没有hadoop环境，这个不是程序错误，也不影响任何功能*/

  }
}