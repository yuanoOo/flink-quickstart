package cn.jxau.yuan.scala.yuan.scala.kudu

import java.util.{Properties, UUID}

import Sink.KuduSink
import es.accenture.flink.Utils.RowSerializable
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.joda.time
import suishen.message.event.define.PVEvent

/**
  * 1、总是报空指针异常，原因暂时不明确
  *    1、可能版本不正确？
  *    2、the offical example is java, it has problems ?
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

        /* Streaming mode - DataSream API - */
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.addSource(new FlinkKafkaConsumer010[PVEvent.Entity]("pv-event", new AbstractDeserializationSchema[PVEvent.Entity] { override def deserialize(message: Array[Byte]): PVEvent.Entity = PVEvent.Entity.parseFrom(message) }, kafkaProps).setStartFromEarliest())
           .map(event => {
               val dateTime = new time.DateTime(event.getNginxTimeMs)
               val nginxDate = dateTime.toString("yyyyMMdd")
               val nginxHour = dateTime.toString("HH")
               val rowSerializable = new RowSerializable(100)
               rowSerializable.setField(0, nginxDate)
               rowSerializable.setField(1, event.getEventId)
               rowSerializable.setField(2, nginxHour)
               rowSerializable.setField(3, event.getNginxTimeMs)
               rowSerializable.setField(4, event.getAppKey)
               rowSerializable.setField(5, "device_id" + UUID.randomUUID())
               rowSerializable.setField(6, event.getPublish)
               rowSerializable.setField(7, event.getImei)
               rowSerializable.setField(8, event.getMac)
               rowSerializable.setField(9, event.getImsi)
               rowSerializable.setField(10, event.getIdfa)
               rowSerializable.setField(11, event.getUid)
               rowSerializable.setField(12, event.getLat)
               rowSerializable.setField(13, event.getLon)
               rowSerializable.setField(14, "北京")
               rowSerializable.setField(15, "北京")
               rowSerializable.setField(16, event.getCityKey)
               rowSerializable.setField(17, event.getOs)
               rowSerializable.setField(18, event.getOsVersion)
               rowSerializable.setField(19, event.getPkg)
               rowSerializable.setField(20, event.getAppVersionCode)
               rowSerializable.setField(21, event.getSdkVersion)
               rowSerializable.setField(22, event.getAppVersion)
               rowSerializable.setField(23, "212121")
               rowSerializable.setField(24, "212121")
               rowSerializable.setField(25, event.getNetwork)
               rowSerializable.setField(26, event.getCountry)
               rowSerializable.setField(27, event.getDeviceSpec)
               rowSerializable.setField(28, event.getTimeZone)
               rowSerializable.setField(29, event.getServiceProvider)
               rowSerializable.setField(30, event.getLanguage)
               rowSerializable.setField(31, event.getChannel)
               rowSerializable.setField(32, event.getEvent)
               rowSerializable.setField(33, event.getEventTimeMs)
               rowSerializable.setField(34, event.getContentId)
               rowSerializable.setField(35, event.getContentModel)
               rowSerializable.setField(36, "cm")
               rowSerializable.setField(37, "cm")
               rowSerializable.setField(38, "cm")
               rowSerializable.setField(39, "cm")
               rowSerializable.setField(40, "cm")
               rowSerializable.setField(41, event.getPosition)
               rowSerializable.setField(42, event.getModule)
               rowSerializable.setField(43, event.getStartNo)
               rowSerializable.setField(44, event.getArgs)
               rowSerializable.setField(45, "arg")
               rowSerializable.setField(46, "arg")
               rowSerializable.setField(47, "arg")
               rowSerializable.setField(48, "arg")
               rowSerializable.setField(49, "arg")
               rowSerializable.setField(50, "arg")
               rowSerializable.setField(51, "arg")
               rowSerializable.setField(52, "arg")
               rowSerializable.setField(53, "arg")
               rowSerializable.setField(54, event.getClientIp)
               rowSerializable.setField(55, event.getUserAgent)
               rowSerializable.setField(56, event.getX3D)
               rowSerializable.setField(57, event.getY3D)
               rowSerializable.setField(58, event.getZ3D)
               rowSerializable
        })
        .addSink(new KuduSink("node101.bigdata.dmp.local.com:7051,node102.bigdata.dmp.local.com:7051,node103.bigdata.dmp.local.com:7051", "ods_kudu_pv_event_1d",
               Array("nginx_date", "event_id", "nginx_hour", "nginx_time", "app_key"
                      , "device_id", "publish", "imei", "mac", "imsi", "idfa", "uid", "lat", "lon"
                      , "province", "city", "city_key", "os", "os_version", "pkg", "version_code"
                      , "sdk_version", "app_version", "screen_width", "screen_height", "access_network"
                      , "country", "device_spec", "time_zone", "sp", "language", "channel", "event_type"
                      , "event_time", "content_id", "content_model", "project", "p_table", "cm_module", "cm_id"
                      , "alg_from", "position", "module", "start_num", "args", "card_id"
                      , "category_id", "topic_id", "circle_id", "remind_id"
                      , "subject_id", "story_id", "item_id", "section_id", "ip", "user_agent", "x3d", "y3d", "z3d")))

        env.execute()
    }
}
