import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment


/**
  * session window
  * https://blog.csdn.net/lmalds/article/details/52692911
  */
object SessionWindowTest {

    // *************************************************************************
    // main函数
    // *************************************************************************

    def main(args: Array[String]): Unit = {

        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

        //        val source = new KafkaConsumeToptrade()
        //
        //        val indexString = source.indexDataStream(env).name("Index").setParallelism(4)
        //        val indexDataStream = new CommonOperator().mapIndexToDataStreamPOJO(indexString).filter(f => f.lastIndex != 0L && f.totalVolume != 0L).setParallelism(8).name("index filter")
        //
        //        val watermarkIndex = indexDataStream.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[StockIndex] {
        //
        //            var currentMaxTimestamp = 0L
        //            val maxOutOfOrderness = 10000L
        //
        //            override def getCurrentWatermark: Watermark = {
        //                new Watermark(currentMaxTimestamp - maxOutOfOrderness)
        //            }
        //
        //            override def extractTimestamp(t: StockIndex, l: Long): Long = {
        //                val timestamp = t.time
        //                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp)
        //                timestamp
        //            }
        //        })
        //            .name("index watermark")
        //            .setParallelism(8)
        //
        //
        //        val sessionWindow = watermarkIndex
        //            .keyBy(_.code)
        //            .window(EventTimeSessionWindows.withGap(Time.seconds(6)))
        //            .apply(new IndexSessionWindow)
        //            .setParallelism(8)
        //
        //
        //        sessionWindow.print().setParallelism(1)
        //
        //        env.execute()

    }

    // *************************************************************************
    // SessionWindow Function
    // *************************************************************************
    //    class IndexSessionWindow extends RichWindowFunction[StockIndex, (String, String, String, String, String, Int), String, TimeWindow] {
    //
    //        var state: ValueState[IndexSumTest] = null
    //        var size = 0
    //
    //        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    //
    //        override def open(config: Configuration): Unit = {
    //
    //            state = getRuntimeContext.getState(new ValueStateDescriptor[IndexSumTest]("snapshot State", classOf[IndexSumTest], null))
    //        }
    //
    //        override def apply(key: String, window: TimeWindow, input: Iterable[StockIndex], out: Collector[(String, String, String, String, String, Int)]): Unit = {
    //            //init
    //            if (state.value() == null) {
    //                state.update(IndexSumTest(0))
    //            } else {
    //                size = state.value().size
    //            }
    //
    //            val list = input.toList.sortBy(_.time)
    //
    //            val window_start_time = format.format(window.getStart)
    //            val window_end_time = format.format(window.getEnd)
    //            val window_size = input.size
    //
    //            size = size + window_size
    //            state.update(IndexSumTest(size))
    //
    //
    //            out.collect((key, window_start_time, window_end_time, format.format(list.head.time), format.format(list.last.time), size))
    //        }
    //    }

    // *************************************************************************
    // Case Class
    // *************************************************************************
    //    case class IndexSumTest(size: Int)

}