## Flink中状态使用指南

### 状态官方文档

### Dynamic Scaling (动态扩展)
https://blog.csdn.net/lmalds/article/details/73457767

- Flink中的state是每一个subTask(一个operator有多少并行度,就有多少subTask)都有一个state
- 主要体现在:operator的并行度改变时,subTask的state如何redistribute, 并且从而恢复
- Dynamic Scaling: means the capability to update the operators' parallelism(Flink 1.2), either for keyed state or for non-keyed state.
- 官方文档对flink1.2新特性的介绍,其中就有:
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
  使用Operator State（non-keyed state），每个operator state都绑定到一个并行operator实例(subTask)。
  
  The Kafka Connector is a good motivating example for the use of Operator State in Flink. 
  Each parallel instance of the Kafka consumer maintains a map of topic partitions and offsets as its Operator State.
  
  The Operator State interfaces support redistributing state among parallel operator instances when the parallelism is changed. There can be different schemes for doing this redistribution.
  Operator State interfaces支持在并行性更改时在并行operator实例(subTask)之间重新分配状态。可以有不同的方案来进行此重新分配.

#### Using Managed Operator State
- CheckpointedFunction

  - ListCheckpointed<T extends Serializable>
    - Currently, list-style managed operator state is supported. 
      The state is expected to be a List of serializable objects, independent from each other, thus eligible for redistribution upon rescaling.
      该状态应该是一个可序列化对象的列表，彼此独立，因此可以在rescaling时进行redistribution。 
      In other words, these objects are the finest granularity at which non-keyed state can be redistributed. 
      换句话说，这些对象是可以被redistributed non-keyed state的最精细的粒度.(怎样算可以被redistributed)
      
      Depending on the state accessing method, the following redistribution schemes are defined:
      根据访问状态方法，定义了以下redistribution方案：
      
        - Even-split redistribution: 
          Each operator returns a List of state elements. 
          每个operator都返回一个状态元素列表。
          
          The whole state is logically a concatenation of all lists. 
          整个状态在逻辑上是所有列表的串联。
          
          On restore/redistribution, the list is evenly divided into as many sublists as there are parallel operators. 
          在restore/redistribution时，列表被平均分成与并行运算符一样多的子列表。
          
          Each operator gets a sublist, which can be empty, or contain one or more elements. 
          每个运算符都会获得一个子列表，该子列表可以为空，也可以包含一个或多个元素。
          
          As an example, if with parallelism 1 the checkpointed state of an operator contains elements element1 and element2, when increasing the parallelism to 2, element1 may end up in operator instance 0, while element2 will go to operator instance 1.
          例如，如果使用并行性1，则运算符的检查点状态包含元素element1和element2，当将并行性增加到2时，element1可能最终在运算符实例0中，而element2将转到运算符实例1。
          
        - Union redistribution: 
           Each operator returns a List of state elements. 
           The whole state is logically a concatenation of all lists. 
           
           On restore/redistribution, each operator gets the complete list of state elements.
           在restore/redistribution时，每个运算符都会获得完整的状态元素列表。


### Queryable State

### 有状态函数

### 无状态函数