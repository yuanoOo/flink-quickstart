package cn.jxau.yuan.scala.yuan.scala

import java.util.Properties

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import suishen.message.event.define.PVEvent

/**
  * @author zhaomingyuan
  * @date 18-7-31
  * @time 下午3:20
  */
object FlinkKafkaExample {

    private val BOOTSTRAP_SERVERS = "node104.bigdata.dmp.local.com:9092,node105.bigdata.dmp.local.com:9092,node106.bigdata.dmp.local.com:9092"

    def main(args: Array[String]): Unit = {
        val kafkaProps = new Properties
        kafkaProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS)
        kafkaProps.setProperty("group.id", "flink-01")

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.enableCheckpointing(100000, CheckpointingMode.EXACTLY_ONCE)

        val transaction = env.addSource(new FlinkKafkaConsumer010[PVEvent.Entity]("pv-event",
            new AbstractDeserializationSchema[PVEvent.Entity] {
                override def deserialize(message: Array[Byte]): PVEvent.Entity = {
                    PVEvent.Entity.parseFrom(message)
                }
            }, kafkaProps).setStartFromEarliest()
        )
                .uid("pv-event-kafka-source")
                .filter(_ != null)
                .uid("filter null pv-event")
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PVEvent.Entity](Time.milliseconds(100000)) {
                    override def extractTimestamp(entity: PVEvent.Entity): Long = {
                        entity.getEventTimeMs
                    }
                })
                .map(event => PvEvent(event.getEventTimeMs, event.getAppKey, event.getEvent))
                .uid("map pv-event to case class")
                .timeWindowAll(Time.minutes(5))
                .reduce((e1, e2) => PvEvent(e1.time, e1.appKey + e2.appKey, e1.name))
                .uid("reduce app_key operator")
                .print()

        env.execute("local-cluster-flink-kafka-test")
    }

    case class PvEvent(time: Long, appKey: Long, name: String)

}
