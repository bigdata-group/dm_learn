package com.yonyou.spark.example

import java.io.FileInputStream
import org.apache.spark.{SparkConf, SparkContext}

class RddOpt(inStr:String) extends  Serializable{
  @transient
  private val conf = new SparkConf().setMaster("local").setAppName("Rdd test")
  @transient
  private val sc =  new SparkContext(conf)

  val name="san";

  def stop() : Unit = {
    sc.stop()
  }

  /*
    map:原rdd中的每个元素执行指定函数生成新的rdd
   */
  def testMap(): Unit ={
    val inRdd = sc.parallelize(1 to 9, 3)
    val b = inRdd.map(x => x*2)
    val c = inRdd.map(x => (x, 1))
    val d = inRdd.map{x=>{
          (x,1)
      }
    }
    b.foreach(println(_))
    println(b.collect().mkString(","))
    c.foreach(println(_))
    println(c.collect().mkString(","))
    d.foreach(println(_))
  }

  /*
   mapPartitions的处理函数，入参和返回都是Iterator
  */
  def myfunc[T](iter: Iterator[T]) : Iterator[(T, T)] = {
    var res = List[(T, T)]()
    var pre = iter.next
    while (iter.hasNext) {
      val cur = iter.next
      res .::= (pre, cur)
      pre = cur
    }
    res.iterator
  }

  /*
    mapPartitions:map是每条处理，而mapPartitions是每个分区一处理
   */
  def testMapPartitions(): Unit={
    val rdd1 = sc.parallelize(1 to 9, 3)
    val rdd2 = rdd1.mapPartitions(myfunc)
    rdd2.foreach(print(_))

    //或者直接写(后边紧跟大括号而不是小括号,瞎说，后边跟小括号也可以)而不是再另写一个函数
    val rdd3 = rdd1.mapPartitions( x => {
       var result = List[Int]()
       var total = 0
       while(x.hasNext){
         total += x.next()
       }
       result.::(total).iterator
    })
    rdd3.foreach(print(_))

    val rdd4 = rdd1.mapPartitions{
      x =>{
        var res = List[(Int, Int)]()
        var pre = x.next
        while (x.hasNext) {
          val cur = x.next
          res .::= (pre, cur)
          pre = cur
        }
        res.iterator
      }
    }
    rdd4.foreach(println(_));
  }

  /*
   testMapPartitionsWithIndex的处理函数，入参和返回都是Iterator
  */
  def sumEveryPartition(x : Int, iter: Iterator[Int]) : Iterator[String] = {
    val result = List[String]()
    var total = 0;
    while(iter.hasNext){
      total += iter.next()
    }
    result.::(x + "|" + total).iterator
  }

  /*
   testMapPartitionsWithIndex将索引标号能够传入，下边例子输出每个分区的编号|分区和值
  */
  def testMapPartitionsWithIndex(): Unit={
    val rdd1 = sc.parallelize(List(1,2,3,4,5,6,7,8,9,10), 3)
    val rdd2 = rdd1.mapPartitionsWithIndex{
      (x, iter)=>{
        val result = List[String]()
        var total = 0;
        while(iter.hasNext){
          total += iter.next()
        }
        result.::(x + "|" + total).iterator
      }
    }
    rdd2.foreach(println(_))

    val rdd3 = rdd1.mapPartitionsWithIndex(sumEveryPartition)
    rdd3.foreach(println(_))
  }

  /*
  * 原map的key不变，对value进行重新算，所以新生成rdd仍是map的数组
  * */
  def testMapValues(): Unit ={
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))
    val c = b.mapValues("x" + _ + "x")//_指的是遍历的值
    //val d = b.mapValues(x=>x+"_"+x)
    c.foreach(println(_));
    //d.foreach(println(_));
  }

  /*
  * 与map类似，区别是原RDD中的元素经map处理后只能生成一个元素，而原RDD中的元素经flatmap处理后可生成多个元素来构建新RDD。
    举例：对原RDD中的每个元素x产生y个元素（从1到y，y为元素x的值）res12: Array[Int] = Array(1, 1, 2, 1, 2, 3, 1, 2, 3, 4)
    而map的结果是{(1),(1,2),(1,2,3),(1,2,3,4)}
  * 正是“先映射后扁平化”： 扁平化就是最终合并成一个对象
  * */
  def testFlatMap() : Unit = {
    val a = sc.parallelize(1 to 4, 2)
    val b = a.flatMap(x => 1 to x)
    b.foreach(println(_))
  }

  /*
  * flatmap + mapvalue(就是mapvalues(key不变，value用flat的1变多的特性))  (1,2),(1,3),(1,4),(1,5),(3,4),(3,5)
  * */
  def testFlatMapValue() : Unit = {
    val a = sc.parallelize(List((1,2),(3,4),(3,6)));
    val b = a.flatMapValues(x=>x.to(5));
    b.foreach(println)
  }

  /*
  * 先传入两个元素产生一个新的值，新产生的值与RDD中下一个元素再被传递给输入函数直到最后只有一个值为止。
  *
  * */
  def testReduce() : Unit = {
    val c = sc.parallelize(1 to 10)
    val d = c.reduce((x,y)=>x+y)
    println("count============================"+d.toString)
  }

  /*
  * 顾名思义，reduceByKey就是对元素为KV对的RDD中Key相同的元素的Value进行reduce
  * */
  def testReduceByKey() : Unit={
    val a = sc.parallelize(List((1,2),(3,4),(3,6)))
    val b = a.reduceByKey((x,y)=>x+y)
    b.foreach(println)
  }

  /*
  * 函数结果作为key, value为key相同的那些value组成的
  Array(
    (even,CompactBuffer(2, 4, 6, 8)),
    (1,ArrayBuffer(1, 3, 5, 7, 9))
  )
  * */
  def testGroupBy() : Unit={
    val a = sc.parallelize(1 to 9, 3);
    val b = a.groupBy(x => { if (x % 2 == 0) "even" else "odd" })
    b.foreach(println)
  }

  /*
  * 直接相同的key的那些value组成数组
  *
  * */
  def testGroupByKey() : Unit={
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 2)
    val b = a.keyBy(_.length).groupByKey()//给value加上key，key为对应string的长度
    b.foreach(println)
  }

/*
* 第一个参数  排序字段
* 第二个参数  true 正排
* 第三个参数  排序后的rdd的分区个数，默认与排序前分区个数相同
*
* */
  def testSortBy() : Unit={
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "spider", "eagle"), 1)
    val b = a.sortBy(x=>x,false,1)
    println(b.collect().mkString(","))
  }

  /*
  * SortBy的后两个参数  结果Array((397090770,4), (com,3), (iteblog,2), (test,5), (wyp,1))
  * 拉链操作
    zip函数将传进来的两个参数中相应位置上的元素组成一个pair数组。如果其中一个参数元素比较长，那么多余的参数会被删掉
  * */
  def testSortByKey() : Unit={
    val a = sc.parallelize(List("wyp", "iteblog", "com", "397090770", "test"), 2)
    val b = sc.parallelize (1 to a.count.toInt , 2)
    val c = a.zip(b).sortByKey()
    println(c.collect().mkString(","))
  }

  /*
  * 一参：是否放回，true放回，可能会有重复，不放回 false
  * 二参：取多少个(比例)，0-1的小数
  * 三参：seed 此值固定，每次取出是一样的，所以觉得这个字段还是不设置值了。
  *
  * */
  def testSample() : Unit={
    val a = sc.parallelize( 1 to 20 , 3 )
    val b = a.sample( true , 0.8 )
    val c = a.sample( false , 0.8 ).filter(_*2>10)
    println( "RDD a : " + a.collect().mkString( " , " ) )
    println( "RDD b : " + b.collect().mkString( " , " ) )
    println( "RDD c : " + c.collect().mkString( " , " ) )
  }


  /*
  * union:就是将两个RDD进行合并，不去重。
  * intersection:该函数返回两个RDD的交集，并且去重。
  * subtract: 减去在另一个数据出现的，剩余的作为结果
  * */
  def testUnion() : Unit={
    val rdd1 = sc.makeRDD(1 to 2,1);
    val rdd2 = sc.makeRDD(2 to 3,1)
    val rdd3 = rdd1.union(rdd2);
    println(rdd3.collect().mkString(","))

    val rdd4 = rdd1.intersection(rdd2);
    println(rdd4.collect().mkString(","))

    val rdd5 = rdd1.subtract(rdd2);
    println(rdd5.collect().mkString(" , "))
  }

  /*
  * 与sql类似，这里的两个集合的关联是用key关联
  * 左(又)外连接不存在时候None
  * cogroup是全外连接
  * cartesian(otherDataset):对两个RDD中的所有元素进行笛卡尔积操作
  * */
  def testJoin() : Unit={
    //建立一个基本的键值对RDD，包含ID和名称，其中ID为1、2、3、4
    val rdd1 = sc.makeRDD(Array(("1","Spark"),("2","Hadoop"),("3","Scala"),("4","Java")),2)
    //建立一个行业薪水的键值对RDD，包含ID和薪水，其中ID为1、2、3、5
    val rdd2 = sc.makeRDD(Array(("1","30K"),("2","15K"),("3","25K"),("5","10K")),2)

    println("//下面做Join操作，预期要得到（1,×）、（2,×）、（3,×）")   //(2,(Hadoop,15K))
    val joinRDD=rdd1.join(rdd2).collect.foreach(println)

    println("//下面做leftOutJoin操作，预期要得到（1,×）、（2,×）、（3,×）、(4,×）")  //(4,(Java,None)) (2,(Hadoop,Some(15K)))
    val leftJoinRDD=rdd1.leftOuterJoin(rdd2).collect.foreach(println)
    println("//下面做rightOutJoin操作，预期要得到（1,×）、（2,×）、（3,×）、(5,×）") //(2,(Some(Hadoop),15K))
    val rightJoinRDD=rdd1.rightOuterJoin(rdd2).collect.foreach(println)
    // full outer join cogroup   (4,(CompactBuffer(Java),CompactBuffer()))  (2,(CompactBuffer(Hadoop),CompactBuffer(15K)))
    val fullOuterJoinRdd = rdd1.cogroup(rdd2).collect().foreach(println)

    val a = sc.parallelize(1 to 3)
    val b = sc.parallelize(2 to 5)
    val cartesianRDD = a.cartesian(b)
    cartesianRDD.foreach(x => println(x + " "))
    rdd1.cartesian(rdd2).foreach(println)//((1,Spark),(1,30K))
  }

  /*
  * 测试pairRdd的操作
  * */
  def testPairRdd() : Unit={
    var arr = Array((2,3), (1,2), (5,6), (4,5), (8,9), (1,8))
    val pairRdd = sc.parallelize(arr)
    val res1 = pairRdd.countByKey();
    res1.foreach(println)//(5,1) (1,2)
    val res2 = pairRdd.lookup(1);
    res2.foreach(println)//2,8
  }

  /*
  * coalesce(numPartitions，shuffle):对RDD的分区进行重新分区，shuffle默认值为false,当shuffle=false时，不能增加分区数
   目,但不会报错，只是分区个数还是原来的 （不清洗还增加个勺子分区）

   repartition(numPartition):是函数coalesce(numPartition,true)的实现
  * */
  def testCoalesce() : Unit={
    val rdd = sc.parallelize(1 to 16,4)
    val coalesceRDD = rdd.coalesce(3) //当suffle的值为false时，不能增加分区数(即分区数不能从5->7)
    println("重新分区后的分区个数:"+coalesceRDD.partitions.size)//3
  }

  /*
  * glom():将RDD的每个分区中的类型为T的元素转换换数组Array[T]
  * */
  def testGlom() : Unit={
    val rdd = sc.parallelize(1 to 16,4)
    val glomRDD = rdd.glom() //RDD[Array[T]]
    glomRDD.foreach(rdd => println(rdd.getClass.getSimpleName))
  }

  /*
  *
  * randomSplit(weight:Array[Double],seed):根据weight权重值将一个RDD划分成多个RDD,权重越高划分得到的元素较多的几率就越大
  *
  * */
  def testRandomSplit() : Unit={
    val rdd = sc.parallelize(1 to 10)
    val randomSplitRDD = rdd.randomSplit(Array(1.0,2.0,7.0))
    randomSplitRDD(0).foreach(x => print(x +" "))
    randomSplitRDD(1).foreach(x => print(x +" "))
    randomSplitRDD(2).foreach(x => print(x +" "))
  }


  // 调用外部的程序来执行
  def testPipe() : Unit={
    val data = List("hi", "hello", "how", "are", "you")
    data.::("man")
    val dataRDD = sc.makeRDD(data)//out123.txt里有hi hello how are you，如果加一个参数变成sc.makeRDD(data，2)则是how are you，我想应该是只有一个worker的缘故
    val scriptPath = "echo.bat"
    val pipeRDD = dataRDD.pipe(scriptPath)
    print(pipeRDD.collect())
  }




}

object RddOpt extends  Serializable{
  def test():Unit={
    println("test")
  }
  def main(args: Array[String]): Unit = {
    RddOpt.test();
    val lst = List((1,2,3),(1,2,3))
    lst.map(e=>(e._1,e._2+e._3))
    println(("a","b","c")._3)

    val opt = new RddOpt("F:\\1.sql")
//     opt.testMap()
      opt.testMapPartitions()
//    opt.testMapPartitionsWithIndex()
//    opt.testMapValues()
//    opt.testFlatMap()
//    opt.testFlatMapValue()
//    opt.testReduce()
//    opt.testReduceByKey()
//    opt.testGroupBy()
//    opt.testGroupByKey()
//    opt.testSortBy()
//    opt.testSortByKey()
//    opt.testSample()
//    opt.testUnion()
//    opt.testJoin()
//    opt.testPairRdd()
//    opt.testCoalesce()
//    opt.testGlom()
//    opt.testRandomSplit()
//    opt.testPipe()
    opt.stop()
  }

}


