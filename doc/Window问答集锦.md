# Window问答集锦

## 一、如果我设置了allowLateness会对我们的计算结果产生什么样的影响呢？

- 上文中提到的在处理元素的最后会注册一个窗口cleanupTimer，那么这个时间是多少呢? window.maxTimestamp() + allowedLateness; 
  所以我们看到一个窗口存在时长是水位线经过window的最大时间+allowLateness的时间，**因此当水位线大于窗口最大时间后就会触发计算，
  而计算之后状态不会清空，会保留allowedLateness的时长，而此时窗口状态还在保留，所以上游有晚到的数据来一条就会触发一次该窗口的计算
  ，而且每次计算的数据都是该窗口的全量数据，所以业务方要慎用，或者下游要做好相应的去重或更新措施，否则可能会造成结果的不准确**
  
  
## 二、我是用apply(),process()函数或者reduce()这种聚合函数对于window的cost代价有多大的差别？

- 不同的函数最终影响的其实是我们最终window保存数据的state的形式有ListState，也有reducingState…,最终影响了rocksdb和
  checkpoint的大小，不过肯定是能用聚合还是用聚合函数比较好