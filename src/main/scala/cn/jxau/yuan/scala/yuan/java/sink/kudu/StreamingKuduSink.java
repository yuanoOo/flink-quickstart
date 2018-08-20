package cn.jxau.yuan.scala.yuan.java.sink.kudu;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhaomingyuan
 * @date 18-8-17
 * @time 下午7:00
 */
public class StreamingKuduSink {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.fromElements("data1 data2 data3");
//        stream.addSink(new KuduSink("node101.bigdata.dmp.local.com:7051", "ods_kudu_pv_event_1d"));
    }
}
