package cn.jxau.yuan.scala.Flink.StreamTime

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
  * 这个Flink DataStream程序，实现“每15秒的加权平均价--VWAP”
  * source：通过SocketStream模拟kafka消费数据
  * sink：直接print输出到local，以后要实现sink到HDFS以及写到Redis
  * 技术点：
  * 1、采用默认的Processing Time统计每15秒钟的加权平均价
  * 2、采用TumblingProcessingTimeWindow作为窗口，即翻滚窗口，系统时钟，不重叠的范围内实现统计
  * 3、在WindowedStream上实现自定义的apply算法，即加权平均价，而非简单的Aggregation
  */

object TransactionVWap {

  case class Transaction(szWindCode: String, szCode: Long, nAction: String, nTime: String, seq: Long, nIndex: Long, nPrice: Long,
                         nVolume: Long, nTurnover: Long, nBSFlag: Int, chOrderKind: String, chFunctionCode: String,
                         nAskOrder: Long, nBidOrder: Long, localTime: Long
                        )

  def main(args: Array[String]): Unit = {

    /**
      * when Running the program, you should input 2 parameters: hostname and port of Socket
      */
    if (args.length != 2) {
      System.err.println("USAGE:\nSocketTextStreamWordCount <hostname> <port>")
      return
    }

    val hostName = args(0)
    val port = args(1).toInt

    /**
      * Step 1. Obtain an execution environment for DataStream operation
      * set EventTime instead of Processing Time
      */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //Processing time is also the Default TimeCharacteristic
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    /**
      * Step 2. Create DataStream from socket
      */
    val input = env.socketTextStream(hostName, port)

    /**
      * Step 3. Implement '每15秒加权平均价-VWAP' logic
      * Note: windowedStream contains 3 attributes: T=>elements, K=>key, W=>window
      */

    val sumVolumePerMinute = input
      //transform Transaction to tuple(szCode, volume, turnover)
      .map(new VwapField)
      //partition by szCode
      .keyBy(_._1)
      //building Tumbling window for 15 seconds
      .timeWindow(Time.seconds(15))
      //compute VWAP in window
      .apply { (k: Long, w: TimeWindow, T: Iterable[(Long, Long, Long)], out: Collector[(Long, String, String, Double)]) =>
        var sumVolume: Long = 0
        var sumTurnover: Long = 0
        for (elem <- T) {
          sumVolume = sumVolume + elem._2
          sumTurnover = sumTurnover + elem._3
        }
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

        val vwap: Double = BigDecimal(String.valueOf(sumTurnover))
          ./(BigDecimal(String.valueOf(sumVolume)))
          .setScale(2, BigDecimal.RoundingMode.HALF_UP)
          .toDouble

        out.collect((k, format.format(w.getStart), format.format(w.getEnd), vwap))
      }
      .name("VWAP per 15 seconds")


    /**
      * Step 4. Sink the final result to standard output(.out file)
      */
    sumVolumePerMinute.print()


    /**
      * Step 5. program execution
      */

    env.execute("SocketTextStream for sum of volume Example")


  }

  class VwapField extends MapFunction[String, (Long, Long, Long)] {

    def map(s: String): (Long, Long, Long) = {

      val columns = s.split(",")

      val transaction: Transaction = Transaction(columns(0), columns(1).toLong, columns(2), columns(3), columns(4).toLong, columns(5).toLong,
        columns(6).toLong, columns(7).toLong, columns(8).toLong, columns(9).toInt, columns(9), columns(10), columns(11).toLong,
        columns(12).toLong, columns(13).toLong)

      val volume: Long = transaction.nVolume
      val szCode: Long = transaction.szCode
      val turnover: Long = transaction.nTurnover

      (szCode, volume, turnover)
    }

  }

}