package cn.jxau.yuan.scala.yuan.java.state.app;

import cn.jxau.yuan.scala.yuan.java.state.function.CountWithOperatorState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * operator stat example
 * 对比与keyed state
 */
public class OperatorStateMain {

    private static final Logger LOG = LoggerFactory.getLogger(OperatorStateMain.class);

    public static void main(String argsp[]) throws Exception {

        // IDE中启用状态后端
        Configuration conf = new Configuration();
//        conf.setString(CoreOptions.CHECKPOINTS_DIRECTORY, "file:///home/yuan/test/checkpoint");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置checkpoint, checkpoint之间的周期为60000ms
        env.enableCheckpointing(60000);

        // 这里配置的状态存储后端会覆盖flink-conf.yaml中相应的配置
        env.setStateBackend(new FsStateBackend("file:///home/yuan/test/checkpoint/1"));
        CheckpointConfig checkpointConf = env.getCheckpointConfig();

        /**
         * 设置检查点尝试之间的最小暂停。
         * 此设置定义检查点协调器在可以触发与最大并发检查点数相关的另一个检查点之后可以多快触发另一个检查点
         * （请参阅{@link #setMaxConcurrentCheckpoints（int）}）。
         *
         * 如果将最大并发检查点数设置为1，则此设置可以有效地确保在没有检查点进行的情况下经过最短时间。
         *
         * the maximum number of concurrent checkpoints
         * 最大并发检查点数:暂时还不理解这个东西的含义,有待了解
         */
        checkpointConf.setMinPauseBetweenCheckpoints(30000L);
        // checkpoint超时时间, with milliseconds
        checkpointConf.setCheckpointTimeout(10000L);

        /**
         * Enables checkpoints to be persisted externally
         * 允许检查点在外部持久化, 同时设置为即使job被cancel,也保留checkpoint的策略
         */
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        env.fromElements(1L, 2L, 3L, 4L, 5L, 1L, 3L, 4L, 5L, 6L, 7L, 1L, 4L, 5L, 3L, 9L, 9L, 2L, 1L)
                .flatMap(new CountWithOperatorState())
                .setParallelism(2)
                .addSink(new SinkFunction<String>() {
                    @Override
                    public void invoke(String s) {
                        LOG.warn(Thread.currentThread().getName() + "===>" + s);
                    }
                });

        env.execute();
    }

}