## Flink中的计算资源
http://wuchong.me/blog/2016/05/09/flink-internals-understanding-execution-resources/#comments

### 一、Operator Chains

### 二、Task Slot
- 1, 为什么要有Task slot
  - TaskManager是一个JVM进程，并会以独立的线程来执行一个task或多个subtask。
    **为了控制一个TaskManager能接受多少个task，Flink提出了Task Slot的概念。**


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