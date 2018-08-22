package cn.jxau.yuan.scala.yuan.java.sink.kudu;

import cn.jxau.yuan.scala.yuan.java.async.kudu.KuduAsyncFunction;
import cn.jxau.yuan.scala.yuan.scala.kudu.utils.Tuple59;
import kudu.batch.KuduOutputFormat;
import kudu.internal.KuduTuple;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static suishen.message.event.define.PVEvent.Entity;

/**
 * @author zhaomingyuan
 * @date 18-8-17
 * @time 下午7:00
 */
public class StreamingKuduSink {

    private static final String BOOTSTRAP_SERVERS = "node104.bigdata.dmp.local.com:9092,node105.bigdata.dmp.local.com:9092,node106.bigdata.dmp.local.com:9092";

    public static void main(String[] args) throws Exception {
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        kafkaProps.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-01");
        KuduOutputFormat.Conf outputConfig = KuduOutputFormat.Conf
                .builder()
                .masterAddress("node101.bigdata.dmp.local.com:7051,node102.bigdata.dmp.local.com:7051,node103.bigdata.dmp.local.com:7051")
                .tableName("ods_kudu_pv_event_1d")
                .writeMode(KuduOutputFormat.Conf.WriteMode.UPSERT)
                .build();

        /* Streaming mode - DataSream API - */
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<KuduTuple> map = env.addSource(new FlinkKafkaConsumer010<Entity>("pv-event", new AbstractDeserializationSchema<Entity>() {
            @Override
            public Entity deserialize(byte[] message) throws IOException {
                return Entity.parseFrom(message);
            }
        }, kafkaProps).setStartFromEarliest()).setParallelism(1).map((MapFunction<Entity, KuduTuple>) event -> {
            DateTime dateTime = new DateTime(event.getNginxTimeMs());
            String nginxDate = dateTime.toString("yyyyMMdd");
            String nginxHour = dateTime.toString("HH");
            return new Tuple59(
                    nginxDate,
                    event.getEventId(),
                    nginxHour,
                    event.getNginxTimeMs(),
                    event.getAppKey(),
                    "device_id" + UUID.randomUUID(),
                    event.getPublish(),
                    event.getImei(),
                    event.getMac(),
                    event.getImsi(),

                    event.getIdfa(),
                    event.getUid(),
                    event.getLat(),
                    event.getLon(),
                    "北京",
                    "北京",
                    456750,
                    event.getOs(),
                    event.getOsVersion(),
                    event.getPkg(),

                    event.getAppVersionCode(),
                    event.getSdkVersion(),
                    event.getAppVersion(),
                    "212121",
                    "212121",
                    event.getNetwork(),
                    event.getCountry(),
                    event.getDeviceSpec(),
                    event.getTimeZone(),
                    event.getServiceProvider(),

                    event.getLanguage(),
                    event.getChannel(),
                    event.getEvent(),
                    event.getEventTimeMs(),
                    event.getContentId(),
                    event.getContentModel(),
                    "cm",
                    "cm",
                    12306,
                    "cm",

                    40,
                    event.getPosition(),
                    event.getModule(),
                    event.getStartNo(),
                    event.getArgs(),
                    "arg",
                    event.getNginxTimeMs(),
                    event.getNginxTimeMs(),
                    event.getNginxTimeMs(),
                    event.getNginxTimeMs(),

                    event.getNginxTimeMs(),
                    event.getNginxTimeMs(),
                    event.getNginxTimeMs(),
                    event.getNginxTimeMs(),
                    event.getNginxTimeMs(),
//           event.getClientIp,
                    event.getUserAgent(),
                    event.getX3D(),
                    event.getY3D(),
                    event.getZ3D(), "");
        }).setParallelism(1);

        AsyncDataStream.unorderedWait(
                map,
                new KuduAsyncFunction(),
                50000,
                TimeUnit.MILLISECONDS,
                20).setParallelism(1);

        env.execute();
    }
}
