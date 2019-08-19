package com.bigdata.spark.example

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiwenlong on 2018/6/7.
  */
class RemoteTest148(name : String) extends Serializable{

  def print_welcome(): Unit ={
    println("hello " + name)
  }

}

object RemoteTest148 extends  Serializable{
  def main(args: Array[String]): Unit = {
    val test = new RemoteTest148("jiwla");
    test.print_welcome()

    val conf = new SparkConf().setMaster("spark://10.10.4.148:7077").setAppName("Rdd test")
    val sc =  new SparkContext(conf)
    var arr = Array((2,3), (1,2), (5,6), (4,5), (8,9), (1,8))
    val pairRdd = sc.parallelize(arr)
    val res1 = pairRdd.countByKey();
    res1.foreach(println)//(5,1) (1,2)
    val res2 = pairRdd.lookup(1);
    res2.foreach(println)//2,8

  }


}
