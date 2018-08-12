package flink

import org.apache.flink.streaming.api.scala._

object KeyedWindowTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.fromElements(1, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7)
            .keyBy(i => i)
            .countWindow(2)
            .sum(0)
            .setParallelism(1)
            .print().setParallelism(1)

        env.execute()
    }
}
