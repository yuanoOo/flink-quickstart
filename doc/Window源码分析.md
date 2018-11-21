# Window源码分析

# Window Merge

- Since session windows do not have a fixed start and end, they are evaluated differently than tumbling 
  and sliding windows. Internally, a session window operator creates a new window for each arriving record 
  and merges windows together if their are closer to each other than the defined gap. In order to be mergeable, 
  a session window operator requires a merging Trigger and a merging Window Function, such as ReduceFunction, 
  AggregateFunction, or ProcessWindowFunction (FoldFunction cannot merge.)
  
  由于会话窗口没有固定的开始和结束，因此它们的评估方式与翻滚和滑动窗口不同。在内部，会话窗口操作员为每个到达的记录创建
  一个新窗口，如果它们彼此之间的距离比定义的间隙更接近，则将窗口合并在一起。为了可合并，会话窗口运算符需要合并触发器和
  合并窗口函数，例如ReduceFunction，AggregateFunction或ProcessWindowFunction（FoldFunction无法合并。）
  
  
