package cn.jxau.yuan.scala.yuan.scala.kudu

import java.util.{Properties, UUID}

import cn.jxau.yuan.scala.yuan.java.sink.kudu.utils.Tuple59
import kudu.batch.KuduOutputFormat
import kudu.batch.KuduOutputFormat.Conf.WriteMode
import kudu.stream.KuduSink
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.joda.time.DateTime
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
                .map(event2Map)
                .addSink(new KuduSink[Tuple59[String, String, String, Long, Int, String, String, String, String, String,
                        String, Long, String, String, String, String, Int, String, String, String,
                        Int, String, String, String, String, String, String, String, Int, String,
                        String, String, String, Long, String, String, String, String, Int, String,
                        Int, String, Int, Int, String, String, Long, Long, Long, Long,
                        Long, Long, Long, Long, Long, String, Double, Double, Double, String]](outputConfig))

        env.execute()
    }


   val event2Map = (event: PVEvent.Entity) => {
       val dateTime = new DateTime(event.getNginxTimeMs)
       val nginxDate = dateTime.toString("yyyyMMdd")
       val nginxHour = dateTime.toString("HH")
       new Tuple59[String, String, String, Long, Int, String, String, String, String, String,
               String, Long, String, String, String, String, Int, String, String, String,
               Int, String, String, String, String, String, String, String, Int, String,
               String, String, String, Long, String, String, String, String, Int, String,
               Int, String, Int, Int, String, String, Long, Long, Long, Long,
               Long, Long, Long, Long, Long, String, Double, Double, Double, String](
           nginxDate,
           event.getEventId,
           nginxHour,
           event.getNginxTimeMs,
           event.getAppKey,
           "device_id" + UUID.randomUUID(),
           event.getPublish,
           event.getImei,
           event.getMac,
           event.getImsi,

           event.getIdfa,
           event.getUid,
           event.getLat,
           event.getLon,
           "北京",
           "北京",
           456750,
           event.getOs,
           event.getOsVersion,
           event.getPkg,

           event.getAppVersionCode,
           event.getSdkVersion,
           event.getAppVersion,
           "212121",
           "212121",
           event.getNetwork,
           event.getCountry,
           event.getDeviceSpec,
           event.getTimeZone,
           event.getServiceProvider,

           event.getLanguage,
           event.getChannel,
           event.getEvent,
           event.getEventTimeMs,
           event.getContentId,
           event.getContentModel,
           "cm",
           "cm",
           12306,
           "cm",

           40,
           event.getPosition,
           event.getModule,
           event.getStartNo,
           event.getArgs,
           "arg",
           event.getNginxTimeMs,
           event.getNginxTimeMs,
           event.getNginxTimeMs,
           event.getNginxTimeMs,

           event.getNginxTimeMs,
           event.getNginxTimeMs,
           event.getNginxTimeMs,
           event.getNginxTimeMs,
           event.getNginxTimeMs,
//           event.getClientIp,
           event.getUserAgent,
           event.getX3D,
           event.getY3D,
           event.getZ3D, ""
       )
   }
}
