package cn.jxau.yuan.scala.yuan.scala.window.test

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.joda.time.DateTime

import scala.util.Random

/**
  * @author zhaomingyuan
  * @date 18-9-15
  * @time 下午8:36
  */
object SlidingProcessTimeWindowTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.addSource((context: SourceContext[String]) => {while(true) context.collect(new Random().nextInt(100) + ":FRI")})
                .keyBy(s => s.endsWith("FRI"))
                .timeWindow(Time.minutes(1), Time.seconds(5))
                .apply((e, w, iter, coll: Collector[String]) => {
                    println("now   ===> " + convert(DateTime.now().getMillis))
                    println("start ===> " + convert(w.getStart))
                    println("end   ===> " + convert(w.getEnd))
                    println("max   ===> " + convert(w.maxTimestamp()))
                    println(w)
                    var reduce: Long = 0
                    for(e <- iter){
                        reduce += e.substring(0, e.length - 4).toInt
                    }
                    println("reduce ==> " + reduce)
                    coll.collect("aggreation")
                }).setParallelism(1).print().setParallelism(1)

        env.execute()
    }

    def convert(time: Long): String = {
        new DateTime(time).toString("yyyy-MM-dd HH:mm:ss")
    }

    """
      SlidingProcessTimeWindowTest && timeWindow(Time.minutes(1), Time.seconds(5))

      |now   ===> 2018-09-16 15:31:00
      |start ===> 2018-09-16 15:30:00
      |end   ===> 2018-09-16 15:31:00
      |max   ===> 2018-09-16 15:30:59
      |TimeWindow{start=1537083000000, end=1537083060000}
      |reduce ==> 16142472
      |aggreation

      |now   ===> 2018-09-16 15:31:05
      |start ===> 2018-09-16 15:30:05
      |end   ===> 2018-09-16 15:31:05
      |max   ===> 2018-09-16 15:31:04
      |TimeWindow{start=1537083005000, end=1537083065000}
      |reduce ==> 79090006
      |aggreation

      |now   ===> 2018-09-16 15:31:10
      |start ===> 2018-09-16 15:30:10
      |end   ===> 2018-09-16 15:31:10
      |max   ===> 2018-09-16 15:31:09
      |TimeWindow{start=1537083010000, end=1537083070000}
      |reduce ==> 104708559
      |aggreation
    """.stripMargin
}
