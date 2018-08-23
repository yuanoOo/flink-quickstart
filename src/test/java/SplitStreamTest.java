package flink.java;

import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;

public class SplitStreamTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        env.fromElements(1, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7)
//                .keyBy(e -> e)
//                .countWindow(2)
//                .sum(0)
//                .setParallelism(1)
//                .print()
//                .setParallelism(1);

        SplitStream<Integer> split = env.fromElements(1, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7)
                .split((OutputSelector<Integer>) value -> {
                    if (value % 2 == 0)
                        return Collections.singleton("oushu");
                    else
                        return Collections.singleton("jishu");
                });

        split.select("oushu")
                .map(i -> "oushu: " + i)
                .setParallelism(1)
                .print().setParallelism(1)
        ;

        split.select("jishu")
                .map(i -> "jishu: " + i)
                .setParallelism(1)
                .print().setParallelism(1);

        System.out.println(env.getExecutionPlan());
        env.execute();
    }
}
