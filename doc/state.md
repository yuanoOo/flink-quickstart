## Flink中状态使用指南

### 状态官方文档

### Dynamic Scaling (动态扩展)
- Flink中的state是每一个subTask(一个operator有多少并行度,就有多少subTask)都有一个state,需要到"小象"进行复习总结
- 主要体现在:operator的并行度改变时,subTask的state如何redistribute, 并且从而恢复
- Dynamic Scaling: means the capability to update the operators' parallelism(Flink 1.2), either for keyed state or for non-keyed state.
- 五角星官方文档对flink1.2新特性的介绍,其中就有:
  -- https://flink.apache.org/news/2017/02/06/release-1.2.0.html#dynamic-scaling--key-groups

### Manager state VS Raw State (operator state和keyed state又分别分为Manager state和Raw State)

- Manager state
  - Flink’s runtime encodes the states and writes them into the checkpoints.
  - 由Flink Runtime进行管理, API提供的都是这种State, 推荐使用
  
- Raw State
  - 不推荐使用
  - Raw State is state that operators keep in their own data structures. When checkpointed, 
    they only write a sequence of bytes into the checkpoint. Flink knows nothing about the state’s 
    data structures and sees only the raw bytes.

- 说明
All datastream functions can use managed state, but the raw state interfaces can only be used when implementing operators. 
Using managed state (rather than raw state) is recommended, since with managed state Flink is able to automatically 
redistribute state when the parallelism is changed, and also do better memory management.

### keyed state
- Keyed State is always relative to keys and can only be used in functions and operators on a KeyedStream.
  Keyed State始终与key相关，只能在KeyedStream上的functions和operators中使用。
  
  You can think of Keyed State as Operator State that has been partitioned, or sharded, with exactly one state-partition per key. 
  您可以将Keyed State视为已分区或分片的Operator State，每个key只有一个状态分区(一个key只会被散列到一个分区)。
  
  Each keyed-state is logically bound to a unique composite of <parallel-operator-instance, key>, and since each key “belongs” to exactly one parallel instance of a keyed operator, we can think of this simply as <operator, key>.
  每个keyed-state在逻辑上和一个唯一的<parallel-operator-instance(subTask), key>组合相绑定.
 
  Keyed State is further organized into so-called Key Groups. Key Groups are the atomic unit by which Flink can redistribute Keyed State;
  Keyed State进一步被组织成所谓的Key Groups(**被如何组织的?**)。Key Groups是Flink可以重新分配Key State的原子单元
  
  there are exactly as many Key Groups as the defined maximum parallelism. During execution each parallel instance of a keyed operator works with the keys for one or more Key Groups.
  Key Groups与定义的最大并行度完全一样多。在执行期间，keyed-state的每个并行实例(subTask)都使用一个或多个Key Groups中的key.

### operator state
- With Operator State (or non-keyed state), each operator state is bound to one parallel operator instance. 
  The Kafka Connector is a good motivating example for the use of Operator State in Flink. Each parallel instance of 
  the Kafka consumer maintains a map of topic partitions and offsets as its Operator State.
  
  The Operator State interfaces support redistributing state among parallel operator instances when the parallelism
   is changed. There can be different schemes for doing this redistribution.

### Queryable State

### 有状态函数

### 无状态函数