package cn.jxau.yuan.scala.yuan.scala.state

import cn.jxau.yuan.scala.yuan.scala.state.function.BloomFilterInValueState
import org.apache.flink.streaming.api.scala._

/**
  * @author zhaomingyuan
  * @date 18-9-10
  * @time 下午5:45
  */
object TestBloomFilterValueState {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.fromElements(Tuple2(0L, 0L), Tuple2(0L, 0L), Tuple2(0L, 0L))
                .keyBy(0)
                .filter(new BloomFilterInValueState)
                .setParallelism(1)
                .print()
                .setParallelism(1)

        env.execute()
    }
}
