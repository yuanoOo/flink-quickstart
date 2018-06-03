import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

/**
  * 用Flink消费kafka
  */
object ReadingFromKafka {

  private val ZOOKEEPER_HOST = "master:2181,worker1:2181,worker2:2181"
  private val KAFKA_BROKER = "master:9092,worker1:9092,worker2:9092"
  private val TRANSACTION_GROUP = "transaction"

  def main(args: Array[String]) {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 检查点间隔
    env.enableCheckpointing(1000)
    // exactly-once vs. at-least-once: 可以通过调用 setCheckpointingMode() 方法在两个一致性级别之间选择其中之一
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // configure Kafka consumer
    val kafkaProps = new Properties()
    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)

    //topicd的名字是new，schema默认使用SimpleStringSchema()即可
    val transaction = env
      .addSource(
        new FlinkKafkaConsumer010[String]("new", new SimpleStringSchema(), kafkaProps)
      )

    transaction.print()

    env.execute()

  }

}