package cn.jxau.yuan.scala.yuan.test;

import java.util.Properties

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig

/**
  * @author zhaomingyuan
  * @date 18-7-31
  * @time 下午3:20
  */
object FlinkKafka {

    private val BOOTSTRAP_SERVERS = "node104.bigdata.dmp.local.com:9092"

    def main(args: Array[String]): Unit = {
        val kafkaProps = new Properties
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-01")

        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.addSource(new FlinkKafkaConsumer011[String]("flink-01",
            new AbstractDeserializationSchema[String] {
                override def deserialize(message: Array[Byte]): String = {
                    String.valueOf(message)
                }
            }, kafkaProps).setStartFromEarliest()
        )

        env.execute("local-cluster-flink-kafka-test")
    }

    case class PvEvent(time: Long, appKey: Long, name: String)

}
