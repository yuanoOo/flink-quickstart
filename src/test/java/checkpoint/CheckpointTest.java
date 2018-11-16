package checkpoint;

import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import suishen.message.event.define.PVEvent;
import suishen.message.event.define.PVEvent.Entity;

import java.io.IOException;
import java.util.Properties;

public class CheckpointTest {
    private static String BOOTSTRAP_SERVERS = "node104.bigdata.dmp.local.com:9092,node105.bigdata.dmp.local.com:9092,node106.bigdata.dmp.local.com:9092";

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
//        conf.setString(CoreOptions.CHECKPOINTS_DIRECTORY, "file:///home/yuan/test/checkpoint");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1, conf);
        env.enableCheckpointing(10L);
        env.setStateBackend(new FsStateBackend("file:///home/yuan/test/checkpoint"));
        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setMinPauseBetweenCheckpoints(30000L);
        checkpointConf.setCheckpointTimeout(8000L);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

//        env.fromElements(1, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7
//                , 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7
//                , 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7
//                , 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7
//                , 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7
//                , 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7
//                , 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7)
//                .setParallelism(1)
//                .countWindowAll(5)
//                .maxBy(0)
//                .print()
//                .setParallelism(1);

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-01");

        env.addSource(new FlinkKafkaConsumer011<Entity>("pv-event", new AbstractDeserializationSchema<Entity>() {
            @Override
            public Entity deserialize(byte[] message) throws IOException {
                System.out.println(message.length);
                return Entity.parseFrom(message);
            }
        }, kafkaProps)).map(e -> 1).countWindowAll(10).sum(0).print();


        env.execute();
    }
}
