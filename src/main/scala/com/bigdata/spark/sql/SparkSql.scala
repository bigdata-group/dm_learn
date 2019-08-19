package com.bigdata.spark.sql

import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkSql {
  def main(args : Array[String]) : Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSql test")
    val sc = new SparkContext(conf)
    //val sqlContext = new SQLContext(sc)
    val sqlContext = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    sqlContext.udf.register("genPreKey", () => UUID.randomUUID.toString)
    //val path = this.getClass.getClassLoader.getResource("data/people.txt").getPath
    // spark读取本地或hdfs文件，然后映射成临时表，然后用sparksql查询
    val rddPeople = sc.textFile("data/people.txt")

    val schemaString = "name age"
    //val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
    val schema = StructType(Array(StructField("name", StringType, true),StructField("age", IntegerType, true)))
    //val rowRDD = rddPeople.map(_.split(",")).map(p => Row(p(0), Integer.valueOf(p(1))))
    val rowRDD = rddPeople.map(_.split(",")).map(p => Row(p(0), p(1).toInt))

    //创建DataFrame：已有的RDD、结构化数据文件、JSON数据集、Hive表、外部数据库。
    val people = sqlContext.createDataFrame(rowRDD, schema)


    people.createOrReplaceTempView("people")

    //sparkSession.table("people").write.saveAsTable("hive_people02")
    sqlContext.sql("select * from  people").show()

    //people.createOrReplaceTempView("People")
    //people.createGlobalTempView("People")

    //sqlContext.table("People").write.saveAsTable("hive_people02")
 //   sqlContext.sql("select * from  hive_people02").show()

//    val peopleNames = sqlContext.sql("SELECT name,genPreKey() uid FROM global_temp.People").show()
//    //peopleNames.map(t => "Name: ").collect().foreach(println)
//    //r1.toDebugString
//
//    val peoplemsg = sqlContext.sql("SELECT name, age FROM global_temp.People ORDER BY age")
//
//    peoplemsg.show(100)
    // peoplemsg.map(t => t(0) + "," + t(1)).collect().foreach(println)

    sc.stop()

  }
}
