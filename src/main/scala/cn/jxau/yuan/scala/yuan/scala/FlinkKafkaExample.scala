package cn.jxau.yuan.scala.yuan.scala

import java.io.IOException
import java.util.Properties

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaConsumer011}
import org.apache.flink.util.StringUtils
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HTable, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory
import suishen.message.event.define.PVEvent

/**
  * @author zhaomingyuan
  * @date 18-7-31
  * @time 下午3:20
  */
object FlinkKafkaExample {

    private val LOG = LoggerFactory.getLogger(FlinkKafkaExample.getClass)
    private val BOOTSTRAP_SERVERS = "node104.bigdata.dmp.local.com:9092,node105.bigdata.dmp.local.com:9092,node106.bigdata.dmp.local.com:9092"

    def main(args: Array[String]): Unit = {
        val kafkaProps = new Properties
        kafkaProps.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS)
        kafkaProps.setProperty("group.id", "flink-01")

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.enableCheckpointing(100000, CheckpointingMode.EXACTLY_ONCE)

        env.addSource(new FlinkKafkaConsumer010[PVEvent.Entity]("pv-event", new AbstractDeserializationSchema[PVEvent.Entity] { override def deserialize(message: Array[Byte]): PVEvent.Entity = PVEvent.Entity.parseFrom(message) }, kafkaProps).setStartFromEarliest())
            .setParallelism(2)
            .uid("pv-event-kafka-source")
            .filter(event => event != null && event.getNginxTimeMs > 1527584646000L)
            .uid("filter null pv-event")
            .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PVEvent.Entity](Time.milliseconds(1000)) {override def extractTimestamp(entity: PVEvent.Entity): Long = entity.getEventTimeMs})
            .map(event => PvEvent(event.getEventTimeMs, event.getAppKey, event.getEvent, if(!StringUtils.isNullOrWhitespaceOnly(event.getImei)) event.getImei else event.getIdfa))
            .uid("map pv-event to case class")
            .timeWindowAll(Time.minutes(5))
            .reduce((e1, e2) => PvEvent(e1.time, e1.appKey + e2.appKey, e1.name, e1.deviceId))
            .uid("reduce app_key operator")
                .print()
//            .writeUsingOutputFormat(new HBaseOutputFormat())

        env.execute("local-cluster-flink-kafka-test")
    }

    /**
      * extract some filed only for test
      *
      * @param time
      * @param appKey
      * @param name
      */
    case class PvEvent(time: Long, appKey: Long, name: String, deviceId: String)

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
            val put = new Put(Bytes.toBytes(record.appKey + record.deviceId))
            put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("time"), Bytes.toBytes(record.time))
            put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("app_key"), Bytes.toBytes(record.appKey))
            put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("name"), Bytes.toBytes(record.name))
            put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("device_id"), Bytes.toBytes(record.deviceId))
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
