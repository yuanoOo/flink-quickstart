package cn.jxau.yuan.scala.yuan.scala.sink.hbase

import java.io.IOException
import java.util.Properties

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.slf4j.LoggerFactory
import suishen.message.event.define.PVEvent

import scala.collection.JavaConverters._

/**
  * @author zhaomingyuan
  * @date 18-7-31
  * @time 下午3:20
  */
object HBaseSinkNoBatch {

    private val LOG = LoggerFactory.getLogger(HBaseSinkNoBatch.getClass)
    private val BOOTSTRAP_SERVERS = "node104.bigdata.dmp.local.com:9092,node105.bigdata.dmp.local.com:9092,node106.bigdata.dmp.local.com:9092"

    def main(args: Array[String]): Unit = {
        val kafkaProps = new Properties
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-01")

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
        env.enableCheckpointing(100000, CheckpointingMode.EXACTLY_ONCE)

        val filter = env.addSource(new FlinkKafkaConsumer011[PVEvent.Entity]("pv-event", new AbstractDeserializationSchema[PVEvent.Entity] { override def deserialize(message: Array[Byte]): PVEvent.Entity = PVEvent.Entity.parseFrom(message) }, kafkaProps).setStartFromEarliest())
            .setParallelism(1)
            .uid("pv-event-kafka-source")
            .filter(event => event != null && event.getNginxTimeMs > 1527584646000L)
            .setParallelism(1)
            .uid("filter null pv-event")

        // window count
//        filter.map(event => PvEvent(event.getEventTimeMs, event.getAppKey, event.getEvent, event.getEventId))
//                .uid("map pv-event to case class")
//                .timeWindowAll(Time.minutes(5L))
//                .sum(1)
//                .print()
//                .name("sink: print")

        // hbase sink
        filter.map(event => PvEvent(event.getEventTimeMs, event.getAppKey, event.getEvent, event.getEventId))
            .setParallelism(1)
            .rescale
            .uid("sink hbase flow map pv-event to case class")
            .writeUsingOutputFormat(new HBaseOutputFormat())
            .setParallelism(1)
            .name("sink hbase")

        env.execute("local-cluster-flink-kafka-test")
    }

    /**
      * extract some filed only for test
      *
      * @param time
      * @param appKey
      * @param name
      */
    case class PvEvent(time: Long, appKey: Long, name: String, eventId: String)

    /**
      * This class implements an OutputFormat for HBase.
      */
    @SerialVersionUID(1L)
    private class HBaseOutputFormat extends OutputFormat[PvEvent] {
        private var conf: org.apache.hadoop.conf.Configuration= null
        private var table: HTable = null
        private var taskNumber: String = null
        private var rowNumber = 0

        private val HBASE_ZK_QURUM = "node101.bigdata.dmp.local.com:2181,node102.bigdata.dmp.local.com:2181,node103.bigdata.dmp.local.com:2181"
        private val HBASE_TABLE = "hbase_flink_test"

        override def configure(parameters: Configuration): Unit = {
            conf = HBaseConfiguration.create()
            conf.set("hbase.zookeeper.quorum", HBASE_ZK_QURUM)
        }

        @throws[IOException]
        override def open(taskNumber: Int, numTasks: Int): Unit = {
            table = new HTable(conf, HBASE_TABLE)
            this.taskNumber = String.valueOf(taskNumber)
        }

        @throws[IOException]
        override def writeRecord(record: PvEvent): Unit = {
            println("device_id ===>", record.eventId)
            val put = new Put(Bytes.toBytes(record.eventId))
            put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("time"), Bytes.toBytes(record.time))
            put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("app_key"), Bytes.toBytes(record.appKey))
            put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("name"), Bytes.toBytes(record.name))
            put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("device_id"), Bytes.toBytes(record.eventId))
            rowNumber += 1
            table.put(put)
        }

        @throws[IOException]
        override def close(): Unit = {
            table.flushCommits()
            table.close()
        }
    }
}
