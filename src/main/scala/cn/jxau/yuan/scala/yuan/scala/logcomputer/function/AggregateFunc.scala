package cn.jxau.yuan.scala.yuan.scala.logcomputer.function

import cn.jxau.yuan.scala.yuan.scala.logcomputer.bean.ComputeResult
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import cn.jxau.yuan.scala.yuan.scala.logcomputer.constants.Constants._


import scala.collection.mutable


class AggregateFunc extends WindowFunction[ComputeResult, ComputeResult, Tuple, TimeWindow]{
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[ComputeResult], out: Collector[ComputeResult]): Unit = {
    // 由于keyBy(FIELD_KEY, FIELD_PERIODS), 所以window中的key为Tuple类型
    val periods = key.getField[String](1)

    // 聚合操作
    periods.split(SEP_SEMICOL).foreach((period) => {
      val end = window.getEnd
      val start = end - period.toLong * 1000L

      val tuples = input.filter(_.metaData(FIELD_TIMESTAMP).asInstanceOf[Long] >= start)
      if (tuples.nonEmpty) {
        val tuple = tuples.map(t => {
          val key = t.metaData(FIELD_UNIQUE_ID) + period
          ComputeResult(
            key,
            metaData = mutable.HashMap(
              FIELD_DATASOURCE -> t.metaData(FIELD_DATASOURCE),
              FIELD_TIMESTAMP -> (end - period.toLong * 1000).asInstanceOf[AnyRef],
              FIELD_PERIOD -> period
            ),
            dimensions = t.dimensions,
            values = t.values

          )
        }).reduce(_ + _)
        out.collect(tuple)
      }
    })
  }
}
