package cn.jxau.yuan.scala.yuan.test

import java.util.Properties

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.util.Collector

import scala.collection.immutable
//import suishen.message.event.define.PVEvent
import org.apache.flink.streaming.api.scala._


/**
  * @author zhaomingyuan
  * @date 18-8-2
  * @time 上午11:49
  */
object FlinkKafkaTest {
    private val BOOTSTRAP_SERVERS = "node104.bigdata.dmp.local.com:9092,node105.bigdata.dmp.local.com:9092,node106.bigdata.dmp.local.com:9092"

    def main(args: Array[String]): Unit = {
        val kafkaProps = new Properties
        kafkaProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS)
        kafkaProps.setProperty("group.id", "flink-01")

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val executionConfig = env.getConfig
        executionConfig.setLatencyTrackingInterval(10)

        env.fromElements(Vector(1, 2)).flatMap((s: Vector[Int], c: Collector[Int]) => s.foreach(c.collect))
        env.fromElements("1", "2", "3").split(e => {
            if(e.equals("1")) List("sds") else List("other")
        } ).select("other").print()
        env.execute("local")
    }
}
