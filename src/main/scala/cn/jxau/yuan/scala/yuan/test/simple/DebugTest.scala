package cn.jxau.yuan.scala.yuan.test.simple

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.joda.time.DateTime

import scala.util.Random

/**
  * @author zhaomingyuan
  * @date 18-10-2
  * @time 下午4:57
  */
object DebugTest {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        env.setBufferTimeout(-1)
        env.addSource((context: SourceContext[String]) => {while(true) context.collect(new Random().nextInt(1000) + ":FRI")})
                .filter(e => !e.isEmpty)
                .print()

        env.execute()
    }

    def convert(time: Long): String = {
        new DateTime(time).toString("yyyy-MM-dd HH:mm:ss")
    }
}
