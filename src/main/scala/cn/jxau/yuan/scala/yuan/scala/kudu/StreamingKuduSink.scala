package cn.jxau.yuan.scala.yuan.scala.kudu

import java.util.Properties

import kudu.batch.KuduOutputFormat
import kudu.batch.KuduOutputFormat.Conf.WriteMode
import kudu.stream.KuduSink
import org.apache.flink.api.java.tuple.Tuple
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
                .map(_ => new org.apache.flink.api.java.tuple.Tuple1[String]("20180101"))
                .addSink(new KuduSink[org.apache.flink.api.java.tuple.Tuple1[String]](outputConfig))
        env.execute()
    }


   val event2Map = (event: PVEvent.Entity) => {

   }


    class Tuple59[T0
    , T1
    , T2
    , T3
    , T4
    , T5
    , T6
    , T7
    , T8
    , T9
    , T10
    , T11
    , T12
    , T13
    , T14
    , T15
    , T16
    , T17
    , T18
    , T19
    , T20
    , T21
    , T22
    , T23
    , T24] extends Tuple{
        private val serialVersionUID = 1L

        /** Field 0 of the tuple. */
        var f0: T0 = null
        /** Field 1 of the tuple. */
        var f1: T1 = null
        /** Field 2 of the tuple. */
        var f2: T2 = null
        /** Field 3 of the tuple. */
        var f3: T3 = null
        /** Field 4 of the tuple. */
        var f4: T4 = null
        /** Field 5 of the tuple. */
        var f5: T5 = null
        /** Field 6 of the tuple. */
        var f6: T6 = null
        /** Field 7 of the tuple. */
        var f7: T7 = null
        /** Field 8 of the tuple. */
        var f8: T8 = null
        /** Field 9 of the tuple. */
        var f9: T9 = null
        /** Field 10 of the tuple. */
        var f10: T10 = null
        /** Field 11 of the tuple. */
        var f11: T11 = null
        /** Field 12 of the tuple. */
        var f12: T12 = null
        /** Field 13 of the tuple. */
        var f13: T13 = null
        /** Field 14 of the tuple. */
        var f14: T14 = null
        /** Field 15 of the tuple. */
        var f15: T15 = null
        /** Field 16 of the tuple. */
        var f16: T16 = null
        /** Field 17 of the tuple. */
        var f17: T17 = null
        /** Field 18 of the tuple. */
        var f18: T18 = null
        /** Field 19 of the tuple. */
        var f19: T19 = null
        /** Field 20 of the tuple. */
        var f20: T20 = null
        /** Field 21 of the tuple. */
        var f21: T21 = null
        /** Field 22 of the tuple. */
        var f22: T22 = null
        /** Field 23 of the tuple. */
        var f23: T23 = null
        /** Field 24 of the tuple. */
        var f24: T24 = null

        override def setField[T](value: T, pos: Int): Unit = ???

        override def getField[T](pos: Int): T = ???

        override def getArity: Int = ???

        override def copy[T <: Tuple](): T = ???
    }
}
