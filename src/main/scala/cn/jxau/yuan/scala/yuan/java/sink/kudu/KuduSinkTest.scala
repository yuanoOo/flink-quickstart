package cn.jxau.yuan.scala.yuan.java.sink.kudu

import java.util.Properties

import cn.jxau.yuan.scala.yuan.java.sink.kudu.utils.{KuduOutputFormat, Pv2RowMapping}
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.configuration.{Configuration, CoreOptions}
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.clients.consumer.ConsumerConfig
import suishen.message.event.define.PVEvent

/**
  * @author zhaomingyuan
  * @date 18-9-19
  * @time 上午9:59
  */
object KuduSinkTest {
    private val BOOTSTRAP_SERVERS = "node104.bigdata.dmp.local.com:9092,node105.bigdata.dmp.local.com:9092,node106.bigdata.dmp.local.com:9092"
    private val KAFKA_TOPIC = "pv-event"
    private val KUDU_MASTER = "node101.bigdata.dmp.local.com:7051,node102.bigdata.dmp.local.com:7051,node103.bigdata.dmp.local.com:7051"
    private val KUDU_TABLE = "ods_kudu_pv_event_1d"
    private val KUDU_FLASH_RATE = 1

    def main(args: Array[String]): Unit = {

        val conf = new Configuration
        conf.setString(CoreOptions.CHECKPOINTS_DIRECTORY, "file:///home/yuan/test/checkpoint")
        val env = StreamExecutionEnvironment.createLocalEnvironment(1, conf)

        env.enableCheckpointing(60000L)
        val checkpointConf = env.getCheckpointConfig
        checkpointConf.setMinPauseBetweenCheckpoints(30000L)
        checkpointConf.setCheckpointTimeout(8000L)
        checkpointConf.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

        val consumerProps = new Properties()
        consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-01")

        val consumer = new FlinkKafkaConsumer010[PVEvent.Entity](KAFKA_TOPIC, new AbstractDeserializationSchema[PVEvent.Entity] {
            override def deserialize(message: Array[Byte]): PVEvent.Entity = PVEvent.Entity.parseFrom(message)
        }, consumerProps).setStartFromEarliest()

        env.addSource(consumer)
                .filter(_ != null)
                .map(new Pv2RowMapping)
                .writeUsingOutputFormat(new KuduOutputFormat(KUDU_MASTER, KUDU_TABLE, KUDU_FLASH_RATE))

        env.execute()
    }
}
