###
- 1. org.apache.flink.api.common.functions.RuntimeContext
  - A RuntimeContext contains information about the context in which functions are executed. 
    RuntimeContext包含有关执行函数的上下文的信息。
    
    Each parallel instance of the function will have a context through which it can access static contextual information (such as 
    the current parallelism) and other constructs like accumulators and broadcast variables.
    函数的每个并行实例都有一个上下文，通过它可以访问静态上下文信息（例如当前并行性）和其他结构体，如累加器和广播变量。
   
    A function can, during runtime, obtain the RuntimeContext via a call to {@link AbstractRichFunction#getRuntimeContext()}.
    在运行时，函数可以通过调用{@link AbstractRichFunction＃getRuntimeContext（）}来获取RuntimeContext。
    
- 2、org.apache.flink.runtime.io.network.partition.consumer.InputGate
```java
/**
 * An input gate consumes one or more partitions of a single produced intermediate result.
 *
 * <p> Each intermediate result is partitioned over its producing parallel subtasks; each of these
 * partitions is furthermore partitioned into one or more subpartitions.
 *
 * <p> As an example, consider a map-reduce program, where the map operator produces data and the
 * reduce operator consumes the produced data.
 *
 * <pre>{@code
 * +-----+              +---------------------+              +--------+
 * | Map | = produce => | Intermediate Result | <= consume = | Reduce |
 * +-----+              +---------------------+              +--------+
 * }</pre>
 *
 * <p> When deploying such a program in parallel, the intermediate result will be partitioned over its
 * producing parallel subtasks; each of these partitions is furthermore partitioned into one or more
 * subpartitions.
 *
 * <pre>{@code
 *                            Intermediate result
 *               +-----------------------------------------+
 *               |                      +----------------+ |              +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <=======+=== | Input Gate | Reduce 1 |
 * | Map 1 | ==> | | Partition 1 | =|   +----------------+ |         |    +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+    |
 *               |                      +----------------+ |    |    | Subpartition request
 *               |                                         |    |    |
 *               |                      +----------------+ |    |    |
 * +-------+     | +-------------+  +=> | Subpartition 1 | | <==+====+
 * | Map 2 | ==> | | Partition 2 | =|   +----------------+ |    |         +-----------------------+
 * +-------+     | +-------------+  +=> | Subpartition 2 | | <==+======== | Input Gate | Reduce 2 |
 *               |                      +----------------+ |              +-----------------------+
 *               +-----------------------------------------+
 * }</pre>
 *
 * <p> In the above example, two map subtasks produce the intermediate result in parallel, resulting
 * in two partitions (Partition 1 and 2). Each of these partitions is further partitioned into two
 * subpartitions -- one for each parallel reduce subtask. As shown in the Figure, each reduce task
 * will have an input gate attached to it. This will provide its input, which will consist of one
 * subpartition from each partition of the intermediate result.
 */
```

- 3、在flink1.4的源码中HeapMemorySegment已经没有再使用了(@SuppressWarnings("unused"))，
     无论堆外还是堆内全权交由HybridMemorySegment了
     在早期版本Flink源码中MemorySegmentFactory，还会选择使用Heap还是Hybrid
