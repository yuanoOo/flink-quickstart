package cn.jxau.yuan.scala.yuan.scala.window.test

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{ContinuousProcessingTimeTrigger, ProcessingTimeTrigger}
import org.apache.flink.util.Collector
import org.joda.time.DateTime

import scala.util.Random

/**
  * 简单测试
  *
  * @author zhaomingyuan
  * @date 18-9-15
  * @time 下午6:44
  */
object TumblingProcessTimeWindowTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.enableCheckpointing(600000L)
        env.addSource((context: SourceContext[String]) => {while(true) context.collect(new Random().nextInt(100) + ":FRI")})
                .keyBy(s => s.endsWith("FRI"))
                .timeWindow(Time.days(1))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(2)))
                .apply((e, w, iter, coll: Collector[Long]) => {
                    var i = 0L
                    println("now ===> " + convert(DateTime.now().getMillis))
                    println("start ===> " + convert(w.getStart))
                    println("end ===> " + convert(w.getEnd))
                    println("max ===> " + convert(w.maxTimestamp()))
                    println(w)

                    for (j <- iter){
                        i = i + j.substring(0, j.length - 4).toInt
                        if (i > 200000000) throw new Exception
                    }
                    coll.collect(i)
                }).setParallelism(1).print().setParallelism(1)

        env.execute()
    }

    def convert(time: Long): String = {
        new DateTime(time).toString("yyyy-MM-dd HH:mm:ss")
    }

    """
      Tumbling timeWindow(Time.minutes(1)) && ProcessTime

         |now ===> 2018-09-15 20:11:02
         |start ===> 2018-09-15 20:10:00
         |end ===> 2018-09-15 20:11:00
         |max ===> 2018-09-15 20:10:59
         |TimeWindow{start=1537013400000, end=1537013460000}

         |now ===> 2018-09-15 20:12:00
         |start ===> 2018-09-15 20:11:00
         |end ===> 2018-09-15 20:12:00
         |max ===> 2018-09-15 20:11:59
         |TimeWindow{start=1537013460000, end=1537013520000}

         |now ===> 2018-09-15 20:13:00
         |start ===> 2018-09-15 20:12:00
         |end ===> 2018-09-15 20:13:00
         |max ===> 2018-09-15 20:12:59
         |TimeWindow{start=1537013520000, end=1537013580000}

    """
}
