package cn.jxau.yuan.scala.yuan.scala.window.tolerance;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

/**
 * @author zhaomingyuan
 * @date 18-11-12
 * @time 上午10:38
 */
public class CountWithEventTimeTrigger<W extends Window> extends Trigger<Object, W> {
    private static final long serialVersionUID = 1L;

    private final long maxCount;

    private final ReducingStateDescriptor<Long> stateDesc =
            new ReducingStateDescriptor<>("count", new CountWithEventTimeTrigger.Sum(), LongSerializer.INSTANCE);

    private CountWithEventTimeTrigger(long maxCount) {
        this.maxCount = maxCount;
    }

    @Override
    public TriggerResult onElement(Object element, long timestamp, W window, TriggerContext ctx) throws Exception {
        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
            // if the watermark is already past the window fire immediately
            System.out.println("CountWithEventTimeTrigger EventTime Trigger......");
            return TriggerResult.FIRE; // at the end of the window, trigger and purge this window
        }
        ctx.registerEventTimeTimer(window.maxTimestamp());

        ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
        count.add(1L);
        if (count.get() >= maxCount) {
            count.clear();
            return TriggerResult.FIRE;
        }
        return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onEventTime(long time, W window, TriggerContext ctx) {
        System.out.println("currentTime: " + time);
        if (time == window.maxTimestamp())
            System.out.println("CountWithEventTimeTrigger EventTime Trigger......");

        return time == window.maxTimestamp() ?
                TriggerResult.FIRE :
                TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time, W window, TriggerContext ctx) throws Exception {
        return TriggerResult.CONTINUE;
    }

    @Override
    public void clear(W window, TriggerContext ctx) throws Exception {
        ctx.getPartitionedState(stateDesc).clear();
    }

    @Override
    public boolean canMerge() {
        return true;
    }

    @Override
    public void onMerge(W window, OnMergeContext ctx) throws Exception {
        ctx.mergePartitionedState(stateDesc);
    }

    @Override
    public String toString() {
        return "CountTrigger(" + maxCount + ")";
    }

    /**
     * Creates a trigger that fires once the number of elements in a pane reaches the given count.
     *
     * @param maxCount The count of elements at which to fire.
     * @param <W>      The type of {@link Window Windows} on which this trigger can operate.
     */
    public static <W extends Window> CountWithEventTimeTrigger<W> of(long maxCount) {
        return new CountWithEventTimeTrigger<>(maxCount);
    }

    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }

    }
}
