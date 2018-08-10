 ## Myself QA List

- 1,下面关于checkpoint的参数,有待验证了解,应该是一个比较重要的参数
```
    /**
     * 设置检查点尝试之间的最小暂停。
     * 此设置定义检查点协调器在可以触发与最大并发检查点数相关的另一个检查点之后可以多快触发另一个检查点
     * （请参阅{@link #setMaxConcurrentCheckpoints（int）}）。
     * 
     * 如果将最大并发检查点数设置为1，则此设置可以有效地确保在没有检查点进行的情况下经过最短时间。
     * 
     * the maximum number of concurrent checkpoints
     * 最大并发检查点数:暂时还不理解这个东西的含义,有待了解
     */
    checkpointConf.setMinPauseBetweenCheckpoints(30000L);
```

- 2,flink operator默认并行度是如何产生的

  - 下面这段注释会给出一些信息
    - 默认的并行度x,会导致所有operators都会以x的并行度运行(x个subTask)
      - Setting a parallelism of x here will cause all operators (such as map, batchReduce) to run with x parallel instances.
    - {@link LocalStreamEnvironment} uses by default a value equal to the number of hardware contexts (CPU cores / threads).
```
	/**
	 * Sets the parallelism for operations executed through this environment.
	 * Setting a parallelism of x here will cause all operators (such as map,
	 * batchReduce) to run with x parallel instances. This method overrides the
	 * default parallelism for this environment. The
	 * {@link LocalStreamEnvironment} uses by default a value equal to the
	 * number of hardware contexts (CPU cores / threads). When executing the
	 * program via the command line client from a JAR file, the default degree
	 * of parallelism is the one configured for that setup.
	 *
	 * @param parallelism The parallelism
	 */
```

- 3,Keyed State是如何被组织成Key Groups的,为什么说Key Groups是Flink可以重新分配Key State的原子单元
  - Keyed State is further organized into so-called Key Groups. Key Groups are the atomic unit by which Flink can redistribute Keyed State;
    Keyed State进一步被组织成所谓的Key Groups(**被如何组织的?**)。Key Groups是Flink可以重新分配Key State的原子单元
    
- 4,在Flink Async I/O中,推荐使用directExecutor
  - 在Async I/O的官方scala demo中, 使用的是directExecutor, 但是现在对directExecutor还是不理解.
  
  - For implementations with Futures that have an Executor (or ExecutionContext in Scala) for callbacks, 
    we suggests to use a DirectExecutor, because the callback typically does minimal work, and a DirectExecutor avoids an additional thread-to-thread handover overhead. 
    对于具有执行器(或Scala中的ExecutionContext)回调的Future实现，我们建议使用DirectExecutor，因为回调通常只做很少的工作，而DirectExecutor避免了额外的线程到线程的切换开销。