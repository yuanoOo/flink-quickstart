package cn.jxau.yuan.scala.yuan.scala.table

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.Kafka011JsonTableSource
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.slf4j.LoggerFactory

/**
  * @author zhaomingyuan
  * @date 18-11-16
  * @time 下午3:28
  */
object StreamSQLKafkaSourceExample {
    private val LOG = LoggerFactory.getLogger(StreamSQLExample.getClass)

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    def main(args: Array[String]): Unit = {

        // set up execution environment
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val tEnv = TableEnvironment.getTableEnvironment(env)

//        val orderA: DataStream[Order] = new Kafka011JsonTableSource[]("", "", )

        // register the DataStreams under the name "OrderA" and "OrderB"
//        tEnv.registerDataStream("OrderA", orderA, 'user, 'product, 'amount)

        // union the two tables
        val result = tEnv.sqlQuery(
            "SELECT * FROM OrderA WHERE amount > 2 UNION ALL " +
                    "SELECT * FROM OrderB WHERE amount < 2")
        result.toAppendStream[Order].map(e => {
            LOG.warn(e.amount.toString)
        })

        env.setBufferTimeout(-1).execute()
    }

    // *************************************************************************
    //     USER DATA TYPES
    // *************************************************************************

    case class Order(user: Long, product: String, amount: Int)
}
