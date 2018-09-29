package cn.jxau.yuan.scala.yuan.scala.window.trigger

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger
import org.joda.time.DateTime

import scala.util.Random

/**
  * @author zhaomingyuan
  * @date 18-9-28
  * @time 下午6:21
  */
object DayWindowTriggerTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.addSource((context: SourceContext[String]) => {while(true) context.collect(new Random().nextInt(100) + ":FRI")})
                .keyBy(s => s.endsWith("FRI"))
                .timeWindow(Time.minutes(1))
                .trigger(new EventTimeTrigger)
                .sum(0)

        env.execute()
    }

    def convert(time: Long): String = {
        new DateTime(time).toString("yyyy-MM-dd HH:mm:ss")
    }
}
