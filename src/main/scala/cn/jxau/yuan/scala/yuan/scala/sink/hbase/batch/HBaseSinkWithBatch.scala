package cn.jxau.yuan.scala.yuan.scala.sink.hbase.batch

import java.io.IOException
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.io.OutputFormat
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
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
object HBaseSinkWithBatch {

    private val LOG = LoggerFactory.getLogger(HBaseSinkWithBatch.getClass)
    private val BOOTSTRAP_SERVERS = "node104.bigdata.dmp.local.com:9092,node105.bigdata.dmp.local.com:9092,node106.bigdata.dmp.local.com:9092"

    def main(args: Array[String]): Unit = {
        val kafkaProps = new Properties
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-01")

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
        env.enableCheckpointing(TimeUnit.HOURS.toMillis(1), CheckpointingMode.EXACTLY_ONCE)

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
//
//        // hbase sink
//        filter.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[PVEvent.Entity](Time.milliseconds(1000)) {override def extractTimestamp(entity: PVEvent.Entity): Long = entity.getEventTimeMs})
//            .map(event => PvEvent(event.getEventTimeMs, event.getAppKey, event.getEvent, event.getEventId))
//            .uid("sink hbase flow map pv-event to case class")
//            .writeUsingOutputFormat(new HBaseOutputFormat())
//            .name("sink hbase")


        // window agg batch for sink
        filter.map(event => PvEvent(event.getEventTimeMs, event.getAppKey, event.getEvent, event.getEventId))
                .keyBy(_.appKey.toString)
                .countWindow(1000)
                .apply(new WindowFunction[PvEvent, Array[PvEvent], String, GlobalWindow] {
                    override def apply(key: String, window: GlobalWindow, input: Iterable[PvEvent], out: Collector[Array[PvEvent]]): Unit = {
                        out.collect(input.toArray)
                    }
                })
                .setParallelism(1)
                .writeUsingOutputFormat(new HBaseBatchOutputFormat)
                .setParallelism(1)

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


    /**
      * This class implements an OutputFormat for HBase.
      */
    @SerialVersionUID(1L)
    private class HBaseBatchOutputFormat extends OutputFormat[Array[PvEvent]] {
        private var conf: org.apache.hadoop.conf.Configuration= null
        private var table: HTable = null
        private var taskNumber: String = null

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
        override def writeRecord(record: Array[PvEvent]): Unit = {
            val puts = record.map(record => {
                val put = new Put(Bytes.toBytes(record.eventId))
                put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("time"), Bytes.toBytes(record.time))
                put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("app_key"), Bytes.toBytes(record.appKey))
                put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("name"), Bytes.toBytes(record.name))
                put.addColumn(Bytes.toBytes("T"), Bytes.toBytes("device_id"), Bytes.toBytes(record.eventId))
                put
            }).toList
            table.batch(puts.asJava)
        }

        @throws[IOException]
        override def close(): Unit = {
            table.flushCommits()
            table.close()
        }
    }
}
