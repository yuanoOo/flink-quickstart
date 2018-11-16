package cn.jxau.yuan.scala.yuan.scala.state

import cn.jxau.yuan.scala.yuan.scala.state.function.BloomFilterInValueState
import org.apache.flink.configuration.{Configuration, CoreOptions}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}

import scala.util.Random

/**
  * @author zhaomingyuan
  * @date 18-9-17
  * @time 下午6:46
  */
object BloomFilterStateTest {

    def main(args: Array[String]): Unit = {

        //--------------------本地容错配置----------------------//
        val conf = new Configuration
//        conf.setString(CoreOptions.CHECKPOINTS_DIRECTORY, "file:///home/yuan/test/checkpoint")
        val env = StreamExecutionEnvironment.createLocalEnvironment(1, conf)
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE)
        env.setStateBackend(new FsStateBackend("file:///home/yuan/test/checkpoint"))

        env.addSource((context: SourceContext[String]) => {while(true) context.collect(new Random().nextInt(1000) + ":FRI")})
                .setParallelism(1)
                .keyBy(s => s.endsWith(":FRI"))
                .filter(new BloomFilterInValueState)
                .setParallelism(1)
                .addSink(new SinkFunction[String] {})
                .setParallelism(1)

        env.execute()
    }
}
