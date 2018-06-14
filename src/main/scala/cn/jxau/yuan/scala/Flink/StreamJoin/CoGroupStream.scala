package cn.jxau.yuan.scala.Flink.StreamJoin

import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


object CoGroupStream {

  case class Transaction(szWindCode:String, szCode:Long, nAction:String, nTime:String, seq:Long, nIndex:Long, nPrice:Long,
                         nVolume:Long, nTurnover:Long, nBSFlag:Int, chOrderKind:String, chFunctionCode:String,
                         nAskOrder:Long, nBidOrder:Long, localTime:Long
                        )

  case class MarketInfo(szCode : Long, nActionDay : String, nTime : String, nMatch : Long)


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

    val coGroupedStreams = eventTransactionStream
      .coGroup(eventMarketStream)
      .where(_._1)
      .equalTo(_._1)
      .window(TumblingEventTimeWindows.of(Time.seconds(10)))
      .apply {
        (t1: Iterator[(Long, Long, Long, Long)], t2: Iterator[(Long, Long, Long)], out: Collector[(Long, String, String, Long, Long)]) =>

          val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

          val listOut = new ListBuffer[(Long, String, String, Long, Long, Long)]

          //将Iterator的元素赋值给一个ListBuffer
          val l1 = new ListBuffer[(Long,Long,Long,Long)]
          while(t1.hasNext){
            l1.append(t1.next())
          }

          val l2 = new ListBuffer[(Long,Long,Long)]
          while(t2.hasNext){
            l2.append(t2.next())
          }

          //遍历每个ListBuffer，将coGroup后的所有结果进行判断，只取Transaction的时间-Snapshot的时间between 0 和3000（ms）
          for(e1 <- l1){
            for(e2 <- l2){

              if(e1._2 - e2._2 >=0 && e1._2 - e2._2 <= 3 * 1000){
                listOut.append((e1._1,"tranTime: "+format.format(e1._2),"markTime: "+ format.format(e2._2),e1._3,e2._3, e1._4))
                //out.collect(e1._1,"tranTime: "+format.format(e1._2),"markTime: "+ format.format(e2._2),e1._3,e2._3)
              }
            }
          }
          //需要将ListBuffer中的结果按照Transaction时间进行排序
          val l : ListBuffer[(Long, String, String, Long, Long, Long)] = listOut.sortBy(_._2)

          //测试是否按照transactionTime进行排序
          l.foreach(f => println("排序后的结果集：" + f))

          var fundFlowIn : Long = 0
          var fundFlowOut : Long= 0
          var InOutState : Int= 1

          /**
            * 实施“资金流量”的逻辑：
            * 如果交易的价格 > 上一快照的价格，则资金流入
            * 如果交易的价格 < 上一快照的价格，则资金流出
            * 如果交易的价格 = 上一快照的价格，则要看上一交易是属于流入还是流出，如果上一交易是流入，则流入，流出则流出
            * 如果第一笔交易的价格与上一快照的价格相等，则默认资金流入
            */
          for(item <- l) {
            if (item._4 > item._5) {
              fundFlowIn = fundFlowIn + item._6
              InOutState = 1
            } else if (item._4 < item._5) {
              fundFlowOut = fundFlowOut + item._6
              InOutState = 0
            } else {
              if (InOutState == 1) {
                fundFlowIn = fundFlowIn + item._6
                InOutState = 1
              } else {
                fundFlowOut = fundFlowOut + item._6
                InOutState = 0
              }
            }
            //out.collect(item._1,item._2,item._3,item._4,item._5)
          }

          if(l.nonEmpty) {
            val szCode = l.head._1
            val tranStartTime = l.head._2
            val tranEndTime = l.last._2

            out.collect(szCode,tranStartTime,tranEndTime,fundFlowIn, fundFlowOut)
          }
      }
      .name("coGroupedStream Test")


    /**
      * 4、标准输出
      * 4、Specify where to put the results of your computations
      */
    coGroupedStreams.print()


    /**
      * 5、执行程序
      * 5、Trigger the program execution
      */
    env.execute("2 DataStream coGroup")

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