package cn.jxau.yuan.scala.Flink.StreamJoin

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector


/**
  * Created by zhe on 2016/6/21.
  *
  * 详情：https://blog.csdn.net/lmalds/article/details/51743038，有如何提交运行
  */
object JoinedOperation {
  case class Transaction(szWindCode:String, szCode:Long, nAction:String, nTime:String, seq:Long, nIndex:Long, nPrice:Long,nVolume:Long, nTurnover:Long, nBSFlag:Int, chOrderKind:String, chFunctionCode:String,nAskOrder:Long, nBidOrder:Long, localTime:Long)

  case class MarketInfo(szCode : Long, nActionDay : String, nTime : String, nMatch : Long)


  case class Market(szCode : Long, eventTime : String, nMatch : Long)

  def main(args: Array[String]): Unit = {
    /**
      * 参数包含3个：hostname，port1，port2
      * port1：作为Transaction的输入流（例如nc -lk 9000，然后输入参数指定9000）
      * port2：作为Market的输入流（例如nc -lk 9999，然后输入参数指定9999）
      */
    if(args.length != 3){
      System.err.println("USAGE:\nSocketTextStream <hostname> <port1> <port2>")
      return
    }
    val hostname = args(0)
    val port1 = args(1).toInt
    val port2 = args(2).toInt

    /**
      * 1、指定运行环境，设置EventTime
      * 1、Obtain an execution environment
      */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /**
      * 2、创建初始化数据流：Transaction与market
      * 2、Load/create the initial data
      */
    val inputTransaction = env.socketTextStream(hostname, port1)
    val inputMarket = env.socketTextStream(hostname, port2)

    /**
      * 3、实施“累计资金流量”，
      *    资金流量（in） = if(当前价格>LastPrice){sum + = nTurnover}elsif(当前价格=LastPrice且最近的一次Transaction的价格<>LastPrice的价格且那次价格>LastPrice){sum += nTurnover}
      *    资金流量（out） = if(当前价格<LastPrice){sum + = nTurnover}elsif(当前价格=LastPrice且最近的一次Transaction的价格<>LastPrice的价格且那次价格<LastPrice){sum += nTurnover}
      * 3、Specify transformations on this data
      */
    val transactionDataStream = inputTransaction.map(new TransactionPrice)
    val marketDataStream = inputMarket.map(new MarketPrice)

    val eventMarketStream = marketDataStream.assignAscendingTimestamps(_._2)
    val eventTransactionStream = transactionDataStream.assignAscendingTimestamps(_._2)

    val joinedStreams = eventTransactionStream
      .join(eventMarketStream)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply{
        (t1 : (Long, Long, Long, Long), t2 : (Long, Long, Long), out : Collector[(Long,String,String,Long,Long,Long)]) =>

          val transactionTime = t1._2
          val marketTime = t2._2
          val differ = transactionTime - marketTime

          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

          if(differ >=0 && differ <= 3 * 1000) {
            out.collect(t1._1,format.format(marketTime) + ": marketTime", format.format(transactionTime) + ": transactionTime",t2._3,t1._3,t1._4)
          }
      }

      .name("JoinedStream Test")

    /**
      * 4、标准输出
      * 4、Specify where to put the results of your computations
      */
    joinedStreams.print()


    /**
      * 5、执行程序
      * 5、Trigger the program execution
      */
    env.execute("2 DataStream join")

  }

  class TransactionPrice extends MapFunction[String,(Long, Long, Long, Long)]{
    def map(transactionStream: String): (Long, Long, Long,Long) = {
      val columns = transactionStream.split(",")
      val transaction = Transaction(columns(0),columns(1).toLong,columns(2),columns(3),columns(4).toLong,columns(5).toLong,
        columns(6).toLong,columns(7).toLong,columns(8).toLong,columns(9).toInt,columns(9),columns(10),columns(11).toLong,
        columns(12).toLong,columns(13).toLong)

      val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")

      if(transaction.nTime.length == 8){
        val eventTimeString = transaction.nAction + '0' + transaction.nTime
        val eventTime : Long = format.parse(eventTimeString).getTime
        (transaction.szCode,eventTime,transaction.nPrice,transaction.nTurnover)
      }else{
        val eventTimeString = transaction.nAction + transaction.nTime
        val eventTime = format.parse(eventTimeString).getTime
        (transaction.szCode,eventTime,transaction.nPrice,transaction.nTurnover)
      }
    }
  }

  class MarketPrice extends MapFunction[String, (Long, Long, Long)]{
    def map(marketStream : String) : (Long, Long, Long) = {
      val columnsMK = marketStream.split(",")

      val marketInfo = MarketInfo(columnsMK(0).toLong,columnsMK(1),columnsMK(2),columnsMK(3).toLong)

      val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")

      if(marketInfo.nTime.length == 8){
        val eventTimeStringMarket = marketInfo.nActionDay + '0' + marketInfo.nTime
        val eventTimeMarket = format.parse(eventTimeStringMarket).getTime
        (marketInfo.szCode, eventTimeMarket, marketInfo.nMatch)
      }else{
        val eventTimeStringMarket = marketInfo.nActionDay  + marketInfo.nTime
        val eventTimeMarket = format.parse(eventTimeStringMarket).getTime
        (marketInfo.szCode, eventTimeMarket, marketInfo.nMatch)
      }
    }
  }

}