package cn.jxau.yuan.scala.Flink.state

import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector

/**
  * 计算VWAP标准差
  * 利用Flink提供的基于key/vale的ListState来完成
  * 将每一分钟的分钟VOLUME以及分钟VWAP放入一个列表，每分钟的计算都是基于此
  * 在每天盘前，都要清空ListState列表，并且盘前的VWAP是0（没有比较）
  */
object TransactionListStateFunction {

  var state: ListState[TransactionListState] = null

  var volume = BigDecimal.valueOf(0.0)
  var VWAP = BigDecimal.valueOf(0.0)

  var listState: java.lang.Iterable[TransactionListState] = null

  class TransactionListStateFunction extends RichFlatMapFunction[(String, Int, String, String, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal),
    (String, Int, String, String, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)] {

    override def open(config: Configuration): Unit = {

      state = getRuntimeContext.getListState[TransactionListState](new ListStateDescriptor[TransactionListState]("VWAP List State", classOf[TransactionListState]))
    }

    override def flatMap(in: (String, Int, String, String, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal),
                         out: Collector[(String, Int, String, String, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal, BigDecimal)]): Unit = {

      /**
        * init
        */
      if (state.get() == null) {
        volume = BigDecimal.valueOf(0.0)
        VWAP = BigDecimal.valueOf(0.0)

        state.add(TransactionListState(volume, VWAP))
      } else {
        //get Iterable
        listState = state.get()
      }

      var VWAP_SD: BigDecimal = BigDecimal.valueOf(0.0)

      /**
        * 盘前，需要将state清空
        */
      if (in._3 < "0930") {
        state.clear()
        state.add(TransactionListState(in._5, in._7))
        listState = state.get()

        VWAP_SD = BigDecimal.valueOf(0.0)
      } else {


        val volume = in._5
        val VWAP = in._7
        val vwap_accum = in._14

        /**
          * 每次先添加当前分钟的VOLUME与VWAP到列表中
          */
        state.add(TransactionListState(volume, VWAP))
        listState = state.get()


        import scala.collection.JavaConverters._
        val scalaState = listState.asScala.toList

        //计算累计VOLUME
        var sum_volume = BigDecimal.valueOf(0.0)
        for (elem <- scalaState) {
          sum_volume = sum_volume.+(elem.volume)
        }

        //开始计算
        var SD = BigDecimal.valueOf(0.0)
        for (elem <- scalaState) {
          val first = elem.volume./(sum_volume)
          val second = BigDecimal.valueOf(Math.pow(elem.VWAP.-(vwap_accum).toDouble, 2))
          SD = SD.+(first.*(second))
        }
        VWAP_SD = BigDecimal.valueOf(Math.sqrt(SD.toDouble)).setScale(4, BigDecimal.RoundingMode.HALF_UP)
      }

      out.collect(in._1, in._2, in._3, in._4, in._5, in._6, in._7, in._8, in._9, in._10, in._11, in._12, in._13, in._14, VWAP_SD, in._16, in._17, in._18, in._19, in._20, in._21)

    }
  }

  case class TransactionListState(volume: BigDecimal, VWAP: BigDecimal)

}