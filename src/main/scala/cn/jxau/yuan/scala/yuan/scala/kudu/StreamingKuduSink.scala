package cn.jxau.yuan.scala.yuan.scala.kudu

import org.apache.flink.streaming.api.scala._

/**
  * @author zhaomingyuan
  * @date 18-8-16
  * @time 下午5:45
  */
object StreamingKuduSink {

    def main(args: Array[String]): Unit = {
        /* Streaming mode - DataSream API - */
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val stream = env.fromElements("data1 data2 data3")
    }
}
