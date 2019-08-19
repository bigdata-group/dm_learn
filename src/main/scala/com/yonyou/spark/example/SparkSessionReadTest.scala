package com.yonyou.spark.example

import java.util.Properties

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.IntegerType

/**
  * sparksql test
  * Created by jiwenlong on 2017/4/11.
  */
object SparkSessionReadTest {
  def main (args: Array[String] ): Unit = {
    //SparkSession.builder.config(conf=SparkConf())
      val spark = SparkSession.builder()
        .appName("read all type test")
        //.config("spark.sql.warehouse.dir","file:///D:/software/spark-2.0.0-bin-hadoop2.7")
        .master("local")
        .enableHiveSupport()
        .getOrCreate()

      //csv to df
      var dataCsv = spark.read.csv("data/people.csv").toDF("name", "age")
      dataCsv = dataCsv.withColumn("tmpAge",dataCsv("age").cast(IntegerType))
      dataCsv = dataCsv.drop("age").withColumnRenamed("tmpAge","age")
      dataCsv.select("name","age").orderBy("age").show()

      // spark sql
      dataCsv.createOrReplaceTempView("people")
      spark.sql("select name,age from people order by age").show()

      // text to df
      val cols:List[String] = List("f_name","f_age")
      import spark.implicits._
      val dsTxt = spark.read.text("data/people.txt").as[String]
      // flatmap会扁平化，最终生成一个数组(name,age,name1,age1)而不是数组的数组((name,age),(name1,age1))
      // 元组才能toDF，所以又map(value=>())  元组用_1,_2可以取
      val dfTxt = dsTxt.map(_.split(",")).map(value=>(value(0),value(1).toInt))toDF(cols:_*)
      dfTxt.select("f_name","f_age").orderBy("f_age").show()

     // df to rdd
      val rdd1 = dfTxt.toJavaRDD.collect()
      println(rdd1)

     // jdbc to df1 自动带field
     val df1 = spark.read
       .format("jdbc")
       .option("driver", "org.postgresql.Driver")
       .option("url", "jdbc:postgresql://172.20.4.124:5432/bq_new")
       .option("dbtable", "common_module")
       .option("user", "bq83")
       .option("password", "bq83")
       .load()
      df1.show()

    // jdbc to df2
    val prop = new java.util.Properties
    prop.setProperty("user","bq83")
    prop.setProperty("password","bq83")
    prop.setProperty("driver","org.postgresql.Driver")
    val df2 = spark.read.jdbc("jdbc:postgresql://172.20.4.124:5432/bq_new","common_module",prop)
    df2.show()

    //data to db
    // 数据库新建表保存  表存在就追加数据
    //df2.write.mode(SaveMode.Append).jdbc("jdbc:postgresql://172.20.4.124:5432/bq_new","common_module_bak",prop)
    //.partitionBy("entity"‌​,"date").format("orc‌​") 保存到spark-warehouse下边了
    //df2.write.mode(SaveMode.Append).saveAsTable("tmp_common_module")
    val datatmp = spark.read.table("tmp_common_module")
    datatmp.createOrReplaceTempView("tmp_common_module")
    spark.sql("select module_code, module_name from tmp_common_module").show();


    // json to df to view

  }

}
