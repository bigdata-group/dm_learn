package com.bigdata.spark.sql

import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlReadHiveTable extends  Serializable{

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("read all type test")
      //.config("spark.sql.warehouse.dir","file:///E:/workspace-bigdata02/dm_learn/spark-warehouse")
      //.config("spark.sql.warehouse.dir","/user/hive/warehouse")
      /// .config("spark.sql.warehouse.dir","spark-warehouseiiiiiii")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()
   // spark.sql("select * from  hive_common_module").show()
    spark.sql("select * from  hive_people02").show()

    // jdbc to df2
//    val prop = new java.util.Properties
//    prop.setProperty("user","bq83")
//    prop.setProperty("password","bq83")
//    prop.setProperty("driver","org.postgresql.Driver")
//    val df2 = spark.read.jdbc("jdbc:postgresql://172.20.4.124:5432/bq_new","common_module",prop)
//    df2.show()

    //
//    //.partitionBy("entity"‌​,"date").format("orc‌​") 保存到spark-warehouse下边了
//    df2.write.mode(SaveMode.Append).saveAsTable("tmp_common_module")
//
//    df2.createOrReplaceTempView("tmp_common_module")
//
//    spark.table("tmp_common_module").write.saveAsTable("hive_common_module")
//
//    spark.sql("select module_code, module_name from tmp_common_module");
  }

}


