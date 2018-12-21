package cn.jxau.yuan.scala.yuan.scala.state

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector


/**
  * @author zhaomingyuan
  * @date 18-8-9
  * @time 下午12:42
  */
object StateTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.enableCheckpointing(10)
        env.setParallelism(1)
        env.setStateBackend(new FsStateBackend("file:///home/yuan/ck"))
        env.fromElements((1, 2), (2, 3), (3, 4), (4, 5), (5, 6))
                .keyBy(_._1)
                .process(new KeyedProcessFunction[Int, (Int, Int), (Int, Int)] {
                    private var priceState: ValueState[Long] = _

                    override def open(parameters: Configuration): Unit = {
                        priceState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("test", createTypeInformation[Long], 0L))
                    }

                    override def processElement(value: (Int, Int), ctx: KeyedProcessFunction[Int, (Int, Int), (Int, Int)]#Context, out: Collector[(Int, Int)]): Unit = {
                        var price: Long = priceState.value()
                        price += 100
                        priceState.update(price)

                        out.collect((value._1, priceState.value().toInt))
                    }
                })
                .print()

        env.execute()
    }
}
