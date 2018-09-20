package cn.jxau.yuan.scala.yuan.java.sink.kudu

import java.util.Properties

import cn.jxau.yuan.scala.yuan.java.sink.kudu.utils.Pv2RowMapping
import kuduNoTuple.Sink.KuduOutputFormat
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.kafka.clients.consumer.ConsumerConfig
import suishen.message.event.define.PVEvent

/**
  * @author zhaomingyuan
  * @date 18-9-18
  * @time 下午3:35
  */
object NoTupleKuduSink {
    private val BOOTSTRAP_SERVERS = "node104.bigdata.dmp.local.com:9092,node105.bigdata.dmp.local.com:9092,node106.bigdata.dmp.local.com:9092"
    private val KUDU_MASTER = "node101.bigdata.dmp.local.com:7051,node102.bigdata.dmp.local.com:7051,node103.bigdata.dmp.local.com:7051"
    private val KUDU_TABLE = "ods_kudu_pv_event_1d"
    private val KUDU_FIELD = Array("nginx_date", "event_id", "nginx_hour", "nginx_time", "app_key"
        , "device_id", "publish", "imei", "mac", "imsi", "idfa", "uid", "lat", "lon"
        , "province", "city", "city_key", "os", "os_version", "pkg", "version_code"
        , "sdk_version", "app_version", "screen_width", "screen_height", "access_network"
        , "country", "device_spec", "time_zone", "sp", "language", "channel", "event_type"
        , "event_time", "content_id", "content_model", "project", "p_table", "cm_module", "cm_id"
        , "alg_from", "position", "module", "start_num", "args", "card_id"
        , "category_id", "topic_id", "circle_id", "remind_id"
        , "subject_id", "story_id", "item_id", "section_id", "ip", "user_agent", "x3d", "y3d", "z3d")

    def main(args: Array[String]): Unit = {
        val kafkaProps = new Properties
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-01")

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
        env.enableCheckpointing(100000, CheckpointingMode.EXACTLY_ONCE)
        env.setBufferTimeout(-1)

        env.addSource(new FlinkKafkaConsumer010[PVEvent.Entity]("pv-event", new AbstractDeserializationSchema[PVEvent.Entity] { override def deserialize(message: Array[Byte]): PVEvent.Entity = PVEvent.Entity.parseFrom(message)}, kafkaProps).setStartFromEarliest())
                .setParallelism(1)
                .map(new Pv2RowMapping)
                .setParallelism(1)
                .writeUsingOutputFormat(new KuduOutputFormat(KUDU_MASTER, KUDU_TABLE, KUDU_FIELD, KuduOutputFormat.APPEND))
                .setParallelism(1)

        env.execute()
    }
}
