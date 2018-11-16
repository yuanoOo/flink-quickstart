package cn.jxau.yuan.scala.yuan.scala.window.test


import cn.jxau.yuan.scala.yuan.scala.window.tolerance.CountWithProcessingTimeTrigger
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.configuration.{Configuration, CoreOptions}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.evictors.{CountEvictor, TimeEvictor}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.joda.time.DateTime

import scala.util.Random

/**
  * @author zhaomingyuan
  * @date 18-11-12
  * @time 下午7:51
  */
object PerKeyPerWindowTest {
    def main(args: Array[String]): Unit = {
        val conf = new Configuration
//        conf.setString(CoreOptions.CHECKPOINTS_DIRECTORY, "file:///home/yuan/test/checkpoint")
        val env = StreamExecutionEnvironment.createLocalEnvironment(1, conf)
        env.setStateBackend(new FsStateBackend("file:///home/yuan/test/checkpoint"))
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
        env.enableCheckpointing(6000L)
        env.addSource((context: SourceContext[String]) => {
            for(i <- 0L until 10333445L)
                context.collect(i + ":FRI")
            //            while(true) context.collect(new Random().nextInt(100) + ":FRI")
        })
//                .keyBy(s => new Random().nextInt(100).toString)
                .keyBy(s => "1")
                .timeWindow(Time.seconds(10000000))
                .trigger(ContinuousProcessingTimeTrigger.of(Time.milliseconds(100)))
                .evictor(TimeEvictor.of(Time.milliseconds(100), true))
//                .trigger(CountWithProcessingTimeTrigger.of(300))
//                .evictor(CountEvictor.of(0, true))
                .process(new MyProcessWindowFunction)
        //                .reduce(new MyReduceFunction, new MyProcessWindowFunction)
        //                .setParallelism(1)

        env.execute()
    }

    def convert(time: Long): String = {
        new DateTime(time).toString("yyyy-MM-dd HH:mm:ss")
    }

    class MyProcessWindowFunction extends ProcessWindowFunction[String, String, String, TimeWindow]{

        var state: ReducingState[Long] = _

        override def open(parameters: Configuration): Unit = {
            println("Window Open......")
            state = getRuntimeContext.getReducingState(
                new ReducingStateDescriptor("sum", new ReduceFunction[Long] {
                    override def reduce(value1: Long, value2: Long): Long = {
                        value1 + value2
                    }
                }, createTypeInformation[Long]))
        }

        override def process(key: String, context: Context, elements: Iterable[String], out: Collector[String]): Unit = {
            val c = elements.iterator.next()
//            println(key + ": " + elements.size)
//            if (elements.size != 300)
                println("Window Buffer Size: " + elements.size)
            //            println("Window Buffer Size: " + elements.size)
            //            state.add(c.substring(0, c.length - 4).toLong)
            //            elements.foreach(e => println("reduce: " + state.get()))
            //            if (state.get() >= 910947028) {
            //                throw new Exception
            //            }

            out.collect(elements.iterator.next())
        }

        override def clear(context: Context): Unit = {
            super.clear(context)
            println("Window Clearing....")
        }

        override def close(): Unit = {
            println("window closing...")
            super.close()
        }
    }

    class MyReduceFunction extends ReduceFunction[String]{
        override def reduce(value1: String, value2: String): String = {

            //            var q = 0L
            //            if (StringUtils.isNumeric(value1)){
            //                q = value1.toLong
            //            }else {
            //                q = value1.substring(0, value1.length - 4).toLong
            //            }
            //            val w = value2.substring(0, value2.length - 4).toLong
            //            (q + w).toString
            null
        }
    }
}
