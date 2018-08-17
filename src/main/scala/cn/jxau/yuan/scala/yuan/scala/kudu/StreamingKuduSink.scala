package cn.jxau.yuan.scala.yuan.scala.kudu

import java.util.Properties

import es.accenture.flink.Sink.KuduSink
import es.accenture.flink.Utils.RowSerializable
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.joda.time
import org.joda.time.DateTime
import suishen.message.event.define.PVEvent

/**
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

        /* Streaming mode - DataSream API - */
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.addSource(new FlinkKafkaConsumer011[PVEvent.Entity]("pv-event", new AbstractDeserializationSchema[PVEvent.Entity] { override def deserialize(message: Array[Byte]): PVEvent.Entity = PVEvent.Entity.parseFrom(message) }, kafkaProps).setStartFromEarliest())
           .map(event => {
                val dateTime = new time.DateTime(event.getNginxTimeMs)
                val nginxDate = dateTime.toString("yyyyMMdd")
               val nginxHour = dateTime.toString("HH")
               val rowSerializable = new RowSerializable
               for(i <- 0 to 100){
                   rowSerializable.setField(i, nginxDate)
                   rowSerializable.setField(i, event.getEventId)
               }
                rowSerializable
        })
        .addSink(new KuduSink("node101.bigdata.dmp.local.com:7051", "ods_kudu_pv_event_1d"))

        env.execute()
    }
}
