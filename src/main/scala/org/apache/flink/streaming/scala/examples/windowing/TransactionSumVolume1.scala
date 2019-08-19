package org.apache.flink.streaming.scala.examples.windowing

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
/**
  * 这是一个简单的Flink DataStream程序，实现每分钟的累计成交量
  * source：通过SocketStream模拟kafka消费数据
  * sink：直接print输出到local，以后要实现sink到HDFS以及写到Redis
  * 技术点：
  *        1、采用EventTime统计每分钟的累计成交量，而不是系统时钟（processing Time）
  *        2、将输入的时间合并并生成Long类型的毫秒时间，以此作为Timestamp，生成Timestamp和WaterMark
  *        3、采用TumblingEventTimeWindow作为窗口，即翻滚窗口，不重叠的范围内实现统计
  */
object TransactionSumVolume1 {
  case class Transaction(szWindCode:String, szCode:Long, nAction:String, nTime:String, seq:Long, nIndex:Long, nPrice:Long,
                                                           nVolume:Long, nTurnover:Long, nBSFlag:Int, chOrderKind:String, chFunctionCode:String,
                                                           nAskOrder:Long, nBidOrder:Long, localTime:Long
                                                          )
  def main(args: Array[String]): Unit = {
    /**
      * when Running the program, you should input 2 parameters: hostname and port of Socket
    */

    val hostName = "localhost"
    val port = 9000
    /**
    * Step 1. Obtain an execution environment for DataStream operation
    * set EventTime instead of Processing Time
    */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //env.getConfig.setAutoWatermarkInterval(2000);
    //env.setParallelism(1);

    /**
  * Step 2. Create DataStream from socket
      * nc -L -p 9000 -v
  */
    val input = env.socketTextStream(hostName,port)
    /**
  * Step 3. Implement '分钟成交量' logic
  */ /**
  * parse input stream to a new Class which is implement the Map function
  */
    val parsedStream = input.map(new EventTimeFunction)
    /**
  * assign Timestamp and WaterMark for Event time: eventTime(params should be a Long type)
  */
     val timeValue = parsedStream.assignAscendingTimestamps(_._2)
      /*val timeValue = parsedStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(Long, Long, Long)] {
      var currentMaxTimestamp = 0L
      val maxOutOfOrderness = 0L//最大允许的乱序时间是10s
      var a : Watermark = null
      val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      override def getCurrentWatermark: Watermark = {
        a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        a
      }
      override def extractTimestamp(t: (Long, Long, Long), l: Long): Long = {
        val timestamp = t._2
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        println("currentMaxTimestamp:" + format.format(currentMaxTimestamp))
        println("timewindow:" + TimeWindow.getWindowStartWithOffset(currentMaxTimestamp, 0, 3000) + "-" + (TimeWindow.getWindowStartWithOffset(currentMaxTimestamp, 0, 3000) + 2999L));
        timestamp
      }
    })*/


   val sumVolumePerMinute = timeValue .keyBy(0).timeWindow(Time.seconds(3)) .sum(3) .name("sum volume per minute")
   //val sumVolumePerMinute = timeValue .keyBy(_._1) .window(TumblingEventTimeWindows.of(Time.seconds(3))) .sum(2) .name("sum volume per minute")
    /**
  * Step 4. Sink the final result to standard output(.out file)
  */
    println("cal.....")
    sumVolumePerMinute.print()
    sumVolumePerMinute.writeAsText("E:\\1.txt")
    /**
  * Step 5. program execution
  */
    env.execute("SocketTextStream for sum of volume Example")
  }

  class EventTimeFunction extends MapFunction[String, (Long, Long, String, Long)] {
    def map(s: String): (Long, Long, String, Long) = {
      println("recieve:"+s)
      val columns = s.split(",")
      val transaction: Transaction = Transaction(columns(0), columns(1).toLong, columns(2), columns(3), columns(4).toLong, columns(5).toLong, columns(6).toLong, columns(7).toLong, columns(8).toLong, columns(9).toInt, columns(9), columns(10), columns(11).toLong, columns(12).toLong, columns(13).toLong)
      val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
      val volume: Long = transaction.nVolume
      val szCode: Long = transaction.szCode
      if (transaction.nTime.length == 8) {
        val eventTimeString = transaction.nAction + '0' + transaction.nTime
        val eventTime: Long = format.parse(eventTimeString).getTime
        (szCode, eventTime, eventTimeString, volume)
      } else {
        val eventTimeString = transaction.nAction + transaction.nTime
        val eventTime = format.parse(eventTimeString).getTime
        (szCode, eventTime,eventTimeString, volume)
      }
    }
  }

}

