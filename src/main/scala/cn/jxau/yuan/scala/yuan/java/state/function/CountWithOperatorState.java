package cn.jxau.yuan.scala.yuan.java.state.function;


import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * 假设事件场景为某业务事件流中有事件 1、2、3、4、5、6、7、8、9 ......
 * 现在，想知道两次事件1之间，一共发生了多少次其他的事件，分别是什么事件，然后输出相应结果。
 * 如下:
 * 事件流 1 2 3 4 5 1 3 4 5 6 7 1 4 5 3 9 9 2 1 ....
 * 输出  4     2 3 4 5
 *      5     3 4 5 6 7
 *     8     4 5 3 9 9 2
 *
 *  ################
 *  在该例子中, 实现了一个有状态的FlatMapFunction, 并展示了如何对状态进行操作
 *
 *  ###############
 *  CountWithOperatorState算子不同的并行度,会有不同的输出结果,因为不同的并行度,每个subTask接受到的数据流也完全不同了(hash)
 */
public class CountWithOperatorState extends RichFlatMapFunction<Long, String> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(CountWithOperatorState.class);

    /**
     * 保存结果状态
     * ListState<>:本身就是一个List,但是其可以被托管给Flink,进行状态的checkpoint,failover
     *
     * 可以将其用transient进行修饰,因为已经托管给flink管理,不在需要进行序列化
     */
    private transient ListState<Long>  checkPointCountList;
    private List<Long> listBufferElements = new ArrayList<>();

    @Override
    public void flatMap(Long r, Collector<String> collector) throws Exception {
        if (r == 1) {
            if (listBufferElements.size() > 0) {
                StringBuffer buffer = new StringBuffer();
                for(int i = 0; i < listBufferElements.size(); i ++) {
                    buffer.append(listBufferElements.get(i) + " ");
                }
                LOG.warn(Thread.currentThread().getName() + buffer.toString());
                collector.collect(buffer.toString());
                listBufferElements.clear();
            }
        } else {
            listBufferElements.add(r);
        }
    }

    /**
     * 对状态进行快照,在flink进行周期性的checkpoint的时候,会调用此方法,进行状态的快照
     *
     * @param functionSnapshotContext
     * @throws Exception
     */
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkPointCountList.clear();
        for (int i = 0 ; i < listBufferElements.size(); i ++) {
            checkPointCountList.add(listBufferElements.get(i));
        }
    }

    /**
     * 1, 初始化状态,从context中获取Flink Manager State
     * 2, 进行restore相关的操作
     *
     * @param functionInitializationContext
     * @throws Exception
     */
    @Override
    public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
        ListStateDescriptor<Long> listStateDescriptor =
                new ListStateDescriptor<>("listForThree", TypeInformation.of(new TypeHint<Long>() {}));

        checkPointCountList = functionInitializationContext.getOperatorStateStore().getListState(listStateDescriptor);
        // 进行状态恢复
        if (functionInitializationContext.isRestored()) {
            for (Long element : checkPointCountList.get()) {
                listBufferElements.add(element);
            }
        }
    }
}