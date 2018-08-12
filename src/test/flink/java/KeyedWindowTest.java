package flink.java;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class KeyedWindowTest {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(1, 1, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7)
                .keyBy(e -> e)
                .countWindow(2)
                .sum(0)
                .setParallelism(1)
                .print()
                .slotSharingGroup("")
                .setParallelism(1);

        env.execute();
    }
}
