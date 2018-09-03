## Flink中的计算资源
http://wuchong.me/blog/2016/05/09/flink-internals-understanding-execution-resources/#comments

### 一、Operator Chains

### 二、Task Slot(不隔离CPU, 每个TaskManager中的Task Slot共享该TaskManager所有的线程)
- 1, 为什么要有Task slot
  - TaskManager是一个JVM进程，并会以独立的线程来执行一个task或多个subtask。
    **为了控制一个TaskManager能接受多少个task，Flink提出了Task Slot的概念。**

- 2, 计算资源Task slot
  - Flink中的计算资源通过Task Slot来定义。每个task slot代表了TaskManager的一个固定大小的资源子集。
    例如，一个拥有3个slot的 TaskManager，会将其管理的内存平均分成三分分给各个slot。
    将资源slot化意味着来自不同job的task不会为了内存而竞争，而是每个task都拥有一定数量的内存储备。
    需要注意的是，**这里不会涉及到CPU的隔离，slot目前仅仅用来隔离task的内存。**

### 三、SlotSharingGroup(default group: 就是每一条pipeline上的subTasks在一个Slot中)
- 1、计算一个Flink App需要多少个Slots
  - 不设置SlotSharingGroup, 即默认所有的task都在default group中（应用的最大并行度）
  - 设置SlotSharingGroup（所有SlotSharingGroup中的最大并行度之和）

- 2、怎么判断operator属于哪个slot共享组呢？
  - 1, 默认情况下，所有的operator都属于默认的共享组default，也就是说默认情况下所有的operator都是可以共享一个slot的。
  - 2, 而当所有input operators具有相同的slot共享组时，该operator会继承这个共享组。
  - 3, 最后，为了防止不合理的共享，用户也能通过API来强制指定operator的共享组.

- 3、优点:
  - 更容易获得更充分的资源利用。如果没有slot共享，那么非密集型操作source/flatmap就会占用同密集型操作keyAggregation/sink一样多的资源。


### 四、CoLocationGroup
- 主要用于迭代流（暂略）

### 五、原理与实现
- Slot的组织关系(Slot, SimpleSlot, SharedSlot)
- Slot(org.apache.flink.runtime.instance.SimpleSlot, org.apache.flink.runtime.instance.SharedSlot)的分配过程


### 总结
- 最核心的是Task Slot，每个slot能运行一个或多个task。为了拓扑更高效地运行，Flink提出了Chaining，
  尽可能地将operators chain在一起作为一个task来处理。为了资源更充分的利用，Flink又提出了SlotSharingGroup，
  尽可能地让多个task共享一个slot。