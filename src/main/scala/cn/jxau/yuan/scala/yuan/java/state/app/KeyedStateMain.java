package cn.jxau.yuan.scala.yuan.java.state.app;

import cn.jxau.yuan.scala.yuan.java.state.function.CountWithKeyedState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 注意keyBy()造成的数据倾斜问题
 */
public class KeyedStateMain {
    private static final Logger LOG = LoggerFactory.getLogger("KeyedStateMain");

    public static void main(String argsp[]) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置checkpoint
        env.enableCheckpointing(60000);
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setMinPauseBetweenCheckpoints(30000L);
        checkpointConf.setCheckpointTimeout(10000L);

        LOG.warn("pc default cores: " + Runtime.getRuntime().availableProcessors());
        /**
         * 本机有四个物理线程, 默认所有算子的并行度均为4
         *
         * 在下面的例子中:keyBy(0), 0 field的所有值均为1, 所有的Tuple2均会被路由到一个subTask上, 因此会产生严重的"数据倾斜"问题.
         * 这也是为什么调整并行度, 输出结果不变的原因.
         */
        env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0)
                .flatMap(new CountWithKeyedState())
                .addSink(new SinkFunction<Tuple2<Long, Long>>() {
                    @Override
                    public void invoke(Tuple2<Long, Long> longLongTuple2) throws Exception {

                    }
                })
                .name("empty sink");

        env.execute("CountOnlyState");
    }
}