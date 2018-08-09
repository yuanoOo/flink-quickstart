## Flink中状态使用指南

### 状态官方文档

### Dynamic Scaling (动态扩展)
- Flink中的state是每一个subTask(一个operator有多少并行度,就有多少subTask)都有一个state,需要到"小象"进行复习总结
- 主要体现在:operator的并行度改变时,subTask的state如何redistribute, 并且从而恢复
- Dynamic Scaling: means the capability to update the operators' parallelism(Flink 1.2), either for keyed state or for non-keyed state.
- 五角星官方文档对flink1.2新特性的介绍,其中就有:
  -- https://flink.apache.org/news/2017/02/06/release-1.2.0.html#dynamic-scaling--key-groups

### Manager state VS Raw State

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

### operator state

### keyed state

### Queryable State

### 有状态函数

### 无状态函数