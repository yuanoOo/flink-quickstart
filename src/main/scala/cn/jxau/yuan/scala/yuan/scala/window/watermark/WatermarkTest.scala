package cn.jxau.yuan.scala.yuan.scala.window.watermark

import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object WatermarkTest {

    def main(args: Array[String]): Unit = {
        val hostName = "127.0.0.1"
        val port = 9999

        val env = StreamExecutionEnvironment.getExecutionEnvironment
//        env.setParallelism(1)
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        val input = env.socketTextStream(hostName,port)

        val inputMap = input.map(f=> {
            val arr = f.split("\\W+")
            val code = arr(0)
            val time = arr(1).toLong
            (code,time)
        })

        val watermark = inputMap.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Long)] {

            var currentMaxTimestamp = 0L
            val maxOutOfOrderness = 1000L//最大允许的乱序时间是10s

            var a : Watermark = null

            val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

            override def getCurrentWatermark: Watermark = {
                a = new Watermark(currentMaxTimestamp - maxOutOfOrderness)
                a
            }

            override def extractTimestamp(t: (String,Long), l: Long): Long = {
                val timestamp = t._2
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
//                println("timestamp:" + t._1 +","+ t._2 + "timestamp: " +format.format(t._2) +","+  currentMaxTimestamp + "currentMaxTimestamp: "+ format.format(currentMaxTimestamp) + ","+ a.toString)
                println(Thread.currentThread().getName + ": timestamp:" + t._1 +"," + "timestamp: " +format.format(t._2) +"," + "currentMaxTimestamp: "+ format.format(currentMaxTimestamp) + ","+ a.toString + format.format(a.getTimestamp))
                timestamp
            }
        })

        val window = watermark
                .keyBy(_._1)
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .apply(new WindowFunctionTest)

        window.print()

        env.execute()
    }

    class WindowFunctionTest extends WindowFunction[(String,Long),(String, Int,String,String,String,String),String,TimeWindow]{

        override def apply(key: String, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[(String, Int,String,String,String,String)]): Unit = {
            val list = input.toList.sortBy(_._2)
            val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
            println("start: " + window.getStart + "end: " + window.getEnd + ", maxStamp: " + window.maxTimestamp())
            out.collect(key,input.size,format.format(list.head._2),format.format(list.last._2),"start: " + format.format(window.getStart),"end: " + format.format(window.getEnd))
        }

    }


}