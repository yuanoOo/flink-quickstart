## Window源码分析
http://wuchong.me/blog/2016/05/25/flink-internals-window-mechanism/

- 1、Flink中的Window对应着Window operator算子，然后由Assigner、Trigger、Evictor进行不同的实现，并进行
     不同的组合，从而形成不同的Window。
- 2、Window本身只是一个Id标识符，window中元素的buffer存储依赖于Flink的状态机制，从而也保证了Window的
     容错性。
- 3、Flink 中窗口机制和时间类型是完全解耦的，也就是说当需要改变时间类型时不需要更改窗口逻辑相关的代码。

### 剖析Window Api
> Window Api三个组件的不同实现的不同组合，可以定义出非常复杂的窗口。Flink 中内置的窗口正是基于这三个组件构成的。

- 1、WindowAssigner
  - 用来决定某个元素被分配到哪个/哪些窗口中去。
- 2、Trigger
  - 决定了一个窗口何时能够被计算或清除，每个窗口都会拥有一个自己的Trigger。
- 3、Evictor
  - 可以译为“驱逐者”。在Trigger触发之后，在窗口被处理之前，Evictor（如果有Evictor的话）
    会用来剔除窗口中不需要的元素，相当于一个filter。

### Window的实现
![](pic/window.png)
- 1、首先上图中的组件都位于一个 **算子(window operator)** 中，数据流源源不断地进入算子，
     每一个到达的元素都会被交给 WindowAssigner。WindowAssigner 会决定元素被放到哪个
     或哪些窗口（window），可能会创建新窗口。因为一个元素可以被放入多个窗口中，所以
     同时存在多个窗口是可能的。注意，Window本身只是一个ID标识符，其内部可能存储了一
     些元数据，如TimeWindow中有开始和结束时间，但是并不会存储窗口中的元素。窗口中的
     元素实际存储在 Key/Value State 中，key为Window，value为元素集合（或聚合值）。
     为了保证窗口的容错性，该实现依赖了 Flink 的 State 机制（参见 state 文档）。

- 2、每一个窗口都拥有一个属于自己的 Trigger，Trigger上会有定时器，用来决定一个窗口何时能够被计算或清除。
     每当有元素加入到该窗口，或者之前注册的定时器超时了，那么Trigger都会被调用。Trigger的返回结果可以是
     continue（不做任何操作），fire（处理窗口数据），purge（移除窗口和窗口中的数据），或者 fire + purge。
     一个Trigger的调用结果只是fire的话，那么会计算窗口并保留窗口原样，也就是说窗口中的数据仍然保留不变，
     等待下次Trigger fire的时候再次执行计算。一个窗口可以被重复计算多次知道它被 purge 了。在purge之前，
     窗口会一直占用着内存。

- 3、当Trigger fire了，窗口中的元素集合就会交给Evictor（如果指定了的话）。Evictor 主要用来遍历窗口中
     的元素列表，并决定最先进入窗口的多少个元素需要被移除。剩余的元素会交给用户指定的函数进行窗口的计
     算。如果没有 Evictor 的话，窗口中的所有元素会一起交给函数进行计算。

- 4、计算函数收到了窗口的元素（可能经过了 Evictor 的过滤），并计算出窗口的结果值，并发送给下游。窗口的
     结果值可以是一个也可以是多个。DataStream API 上可以接收不同类型的计算函数，包括预定义的sum(),min()
     ,max()，还有 ReduceFunction，FoldFunction，还有WindowFunction。WindowFunction 是最通用的计算函数
     ，其他的预定义的函数基本都是基于该函数实现的。

### CountWindow(三组件的使用典范)
- 1, 在Count Window中, 无论是翻滚还是滑动窗口,具体实现都不用分配多个窗口
  - 具体原因待描述
- 2,Slide Time Window需要分配多个窗口

```java
// tumbling count window
public WindowedStream<T, KEY, GlobalWindow> countWindow(long size) {
  return window(GlobalWindows.create())  // create window stream using GlobalWindows
      .trigger(PurgingTrigger.of(CountTrigger.of(size))); // trigger is window size
}
// sliding count window
public WindowedStream<T, KEY, GlobalWindow> countWindow(long size, long slide) {
  return window(GlobalWindows.create())
    .evictor(CountEvictor.of(size))  // evictor is window size
    .trigger(CountTrigger.of(slide)); // trigger is slide size
}
```


### TimeWindow

- 需要多少窗口:
  - 翻滚Time Window: window size为1分钟,则1小时会有60个窗口
  - 滑动Time Window: window size为1分钟, Slide Size为30秒, 则小时会有60 * (1min/ 30sec)个窗口

https://blog.csdn.net/lmalds/article/details/51604501

```java
// tumbling time window
public WindowedStream<T, KEY, TimeWindow> timeWindow(Time size) {
  if (environment.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
    return window(TumblingProcessingTimeWindows.of(size));
  } else {
    return window(TumblingEventTimeWindows.of(size));
  }
}
// sliding time window
public WindowedStream<T, KEY, TimeWindow> timeWindow(Time size, Time slide) {
  if (environment.getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime) {
    return window(SlidingProcessingTimeWindows.of(size, slide));
  } else {
    return window(SlidingEventTimeWindows.of(size, slide));
  }
}
```

- sliding process time window
```java
public class SlidingProcessingTimeWindows extends WindowAssigner<Object, TimeWindow> {
  private static final long serialVersionUID = 1L;
  private final long size;
  private final long slide;
  private SlidingProcessingTimeWindows(long size, long slide) {
    this.size = size;
    this.slide = slide;
  }

  // 为每个元素分配其所属的窗口
  @Override
  public Collection<TimeWindow> assignWindows(Object element, long timestamp) {
    timestamp = System.currentTimeMillis();
    List<TimeWindow> windows = new ArrayList<>((int) (size / slide));

    // slide：窗口滑动间隔
    // 对齐时间戳，
    long lastStart = timestamp - timestamp % slide;

    for (long start = lastStart; start > timestamp - size; start -= slide) {
      // 当前时间戳对应了多个window
      windows.add(new TimeWindow(start, start + size));
    }
    return windows;
  }
  ...
}
public class ProcessingTimeTrigger extends Trigger<Object, TimeWindow> {
  @Override
  // 每个元素进入窗口都会调用该方法
  public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) {
    // 注册定时器，当系统时间到达window end timestamp时会回调该trigger的onProcessingTime方法
    ctx.registerProcessingTimeTimer(window.getEnd());
    return TriggerResult.CONTINUE;
  }
  @Override
  // 返回结果表示执行窗口计算并清空窗口
  public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) {
    return TriggerResult.FIRE_AND_PURGE;
  }
  ...
}
```

###
