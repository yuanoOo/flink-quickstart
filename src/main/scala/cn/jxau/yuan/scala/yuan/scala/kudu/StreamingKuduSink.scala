package cn.jxau.yuan.scala.yuan.scala.kudu

import java.util.Properties

import cn.jxau.yuan.scala.yuan.scala.kudu.utils.Tuple59
import kudu.batch.KuduOutputFormat
import kudu.batch.KuduOutputFormat.Conf.WriteMode
import kudu.stream.KuduSink
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.clients.consumer.ConsumerConfig
import suishen.message.event.define.PVEvent

/**
  * kudu connector: https://github.com/0xNacho/bahir-flink/tree/feature/flink-connector-kudu/flink-connector-kudu
  *
  * @author zhaomingyuan
  * @date 18-8-16
  * @time 下午5:45
  */
object StreamingKuduSink {
    private val BOOTSTRAP_SERVERS = "node104.bigdata.dmp.local.com:9092,node105.bigdata.dmp.local.com:9092,node106.bigdata.dmp.local.com:9092"

    def main(args: Array[String]): Unit = {
        val kafkaProps = new Properties
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-01")
        val outputConfig = KuduOutputFormat.Conf
                   .builder()
                   .masterAddress("node101.bigdata.dmp.local.com:7051,node102.bigdata.dmp.local.com:7051,node103.bigdata.dmp.local.com:7051")
                   .tableName("ods_kudu_pv_event_1d")
                   .writeMode(WriteMode.UPSERT)
                   .build()

        /* Streaming mode - DataSream API - */
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.addSource(new FlinkKafkaConsumer010[PVEvent.Entity]("pv-event", new AbstractDeserializationSchema[PVEvent.Entity] { override def deserialize(message: Array[Byte]): PVEvent.Entity = PVEvent.Entity.parseFrom(message) }, kafkaProps).setStartFromEarliest())
                .map(_ => {
                    val tuple = new Tuple59[String](

                    )
                }
                .addSink(new KuduSink[org.apache.flink.api.java.tuple.Tuple1[String]](outputConfig))
        env.execute()
    }


   val event2Map = (event: PVEvent.Entity) => {

   }
}
