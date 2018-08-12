## Flink中的计算资源
http://wuchong.me/blog/2016/05/09/flink-internals-understanding-execution-resources/#comments

### 一、Operator Chains

### 二、Task Slot
- 1、计算一个Flink App需要多少个Slots
  - 不设置SlotSharingGroup（应用的最大并行度）
  - 设置SlotSharingGroup（所有SlotSharingGroup中的最大并行度之和）

### 三、SlotSharingGroup 与 CoLocationGroup
- 1、默认情况下，Flink 允许subtasks共享slot，条件是它们都来自同一个Job的不同task的subtask。