package cn.jxau.yuan.scala.yuan.scala.state

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._


/**
  * @author zhaomingyuan
  * @date 18-8-9
  * @time 下午12:42
  */
object Accumulators {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.fromElements(1, 2, 3, 4, 5)
                .map(new RichMapFunction[Int, String] {
                    private val numLines = new IntCounter

                    override def map(value: Int): String = {
                        numLines.add(value)
                        value.toString
                    }

                    override def open(parameters: Configuration): Unit = {
                        getRuntimeContext.addAccumulator("number-lines", numLines)
                }
                })
                .print()

        val result: JobExecutionResult = env.execute()
        println("累加器Accumulators结果 ==> " + result.getAccumulatorResult("number-lines"))
        println("job净执行时间 ==> " + result.getNetRuntime(TimeUnit.MILLISECONDS) + "ms")
    }
}
