## Flink Window
http://flink.iteblog.com/dev/windows.html

### key的选择
- key要根据实际的业务场景进行选择，例如可以将事件类型作为key，那么相同事件类型的record将会聚集在一起，
  从而用于window、reduce等操作。
  
- 数据流进行Key By, 后面往往会在keyStream上进行一系列window, agg等操作, 要根据业务场景选择是否KeyBy 
  
- 所以Key不应选择全局唯一性的属性, 因为有多少record，就有多少key Stream（所以并不能进行数据打散），
  就会有多少keyed window，这将是一个巨大的状态。同时窗口，reduce等操作也没有了意义，因为只有一个record。
  
- 从而一句话就是，keyBy不是用来打散数据的，而是用于给流进行分区的（注意这和forward、rebalance等分区函数
  完全不同，他们是对数据流进行分区的）。


### Keyed Window
- 如果是keyed streams，则进入的事件的任意属性都可以用来作为key。keyed stream也将允许你的窗口计算并行的 
  在多个task上进行，因为每个逻辑keyed stream和其他keyed stream是完全独立的所有key相同的元素都会被发送
  到同一个并行的task。
  
- 被key分开的数据流, 还是要落会到Task上进行执行.

### Non-Keyed Window（并行度为1，可怕）
如果是non-keyed streams，则原始stream将不会被拆分为多个逻辑上的streams，
并且所有的窗口计算逻辑都会值被一个task执行，即并行度为1.