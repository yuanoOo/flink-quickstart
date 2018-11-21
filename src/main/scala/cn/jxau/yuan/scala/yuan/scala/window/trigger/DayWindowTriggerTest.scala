package cn.jxau.yuan.scala.yuan.scala.window.trigger

import java.lang

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.util.Collector
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
        env.setParallelism(1)
        env.addSource((context: SourceContext[String]) => {while(true) context.collect(new Random().nextInt(100) + ":FRI")})
                .keyBy(s => s.endsWith("FRI"))
                .countWindow(10)
                .process(new WF())

        env.execute()
    }

    class WF extends ProcessWindowFunction[String, String, Boolean, GlobalWindow]{

        override def process(key: Boolean, context: Context, elements: Iterable[String], out: Collector[String]): Unit = {

        }

        override def clear(context: Context): Unit = {
            println("clearing....")
        }
    }

    def convert(time: Long): String = {
        new DateTime(time).toString("yyyy-MM-dd HH:mm:ss")
    }
}
