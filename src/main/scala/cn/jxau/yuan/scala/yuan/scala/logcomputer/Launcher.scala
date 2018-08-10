package cn.jxau.yuan.scala.yuan.scala.logcomputer

import java.util.Properties

import cn.jxau.yuan.scala.yuan.scala.logcomputer.bean.{ComputeResult, LogEvent}
import cn.jxau.yuan.scala.yuan.scala.logcomputer.function.{AggregateFunc, ApplyComputeRule}
import cn.jxau.yuan.scala.yuan.scala.logcomputer.schema.{ComputeResultSerializeSchema, LogEventDeserializationSchema}
import cn.jxau.yuan.scala.yuan.scala.logcomputer.source.ConfSource
import cn.jxau.yuan.scala.yuan.scala.logcomputer.watermarker.BoundedLatenessWatermarkAssigner
import cn.jxau.yuan.scala.yuan.scala.logcomputer.constants.Constants._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer010, FlinkKafkaProducer010}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig


object Launcher {

  /**
    *
    * @param args:
    * 0: bootstrap Servers
    * 1: groupId
    * 2: consumerTopic
    * 3: retries
    * 4: producerTopic
    * 5: url
    * 6: latency
    */
  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    /* Checkpoint */
    env.enableCheckpointing(60000L)
    val checkpointConf = env.getCheckpointConfig
    checkpointConf.setMinPauseBetweenCheckpoints(30000L)
    checkpointConf.setCheckpointTimeout(8000L)
    checkpointConf.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /* Kafka consumer */
    val consumerProps = new Properties()
    consumerProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, args(0))
    consumerProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, args(1))
    val consumer = new FlinkKafkaConsumer010[LogEvent](
      args(2),
      new LogEventDeserializationSchema,
      consumerProps
    )

    val producerProps = new Properties()
    producerProps.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args(0))
    // 配置重试次数
    producerProps.setProperty(ProducerConfig.RETRIES_CONFIG, args(3))
    val producer =new FlinkKafkaProducer010[ComputeResult](
      args(4),
      new ComputeResultSerializeSchema(args(4)),
      producerProps
    )

    /**
      * at_ least_once 设置
      *
      * Defines whether the producer should fail on errors, or only log them.
      * If this is set to true, then exceptions will be only logged, if set to false,
      * exceptions will be eventually thrown and cause the streaming program to
      * fail (and enter recovery).
      */
    producer.setLogFailuresOnly(false)

    /**
      * If set to true, the Flink producer will wait for all outstanding messages in the Kafka buffers
      * to be acknowledged by the Kafka producer on a checkpoint.
      * This way, the producer can guarantee that messages in the Kafka buffers are part of the checkpoint.
      *
      * flush Flag indicating the flushing mode (true = flush on checkpoint)
      */
    producer.setFlushOnCheckpoint(true)

      /*confStream **/
    val confStream = env.addSource(new ConfSource(args(5)))
      .setParallelism(1)
      .broadcast


    env.addSource(consumer)
      .connect(confStream)
      .flatMap(new ApplyComputeRule)
      .assignTimestampsAndWatermarks(new BoundedLatenessWatermarkAssigner(args(6).toInt))
      .keyBy(FIELD_KEY)
      .window(TumblingEventTimeWindows.of(Time.minutes(1)))
      .reduce(_ + _)
      .keyBy(FIELD_KEY, FIELD_PERIODS)
      .window(SlidingEventTimeWindows.of(Time.minutes(60), Time.minutes(1)))
      .apply(new AggregateFunc())
      .addSink(producer)

    env.execute("log_compute")

  }

}
