package cn.jxau.yuan.scala.yuan.test

import java.util.Properties

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}
import suishen.message.event.define.PVEvent
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}


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
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//        env.enableCheckpointing(100000, CheckpointingMode.EXACTLY_ONCE)

        env.addSource(new FlinkKafkaConsumer011[PVEvent.Entity]("pv-event",
            new AbstractDeserializationSchema[PVEvent.Entity] {
                override def deserialize(message: Array[Byte]): PVEvent.Entity = PVEvent.Entity.parseFrom(message)
            }, kafkaProps).setStartFromEarliest())
                        .filter(e => {
                            println(e)
                            1 == 1
                        })
                .print()

        env.execute("local")
    }
}
