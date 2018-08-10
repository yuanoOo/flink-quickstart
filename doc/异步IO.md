## Async I/O in Flink

### 一, Async I/O API 
  The ResultFuture is completed with the first call of ResultFuture.complete. All subsequent complete calls will be ignored.
  ResultFuture在第一次调用ResultFuture.complete时完成。随后的所有完整呼叫都将被忽略。
  
  ResultFuture.complete()的注释:
  Note that it should be called for exactly one time in the user code. Calling this function for multiple times will cause data lose.
  请注意，应该在用户代码中只调用一次。多次调用此函数将导致数据丢失。
  
  The following two parameters control the asynchronous operations:
  - Timeout: The timeout defines how long an asynchronous request may take before it is considered failed. This parameter guards against dead/failed requests.
              定义了异步请求在被视为失败之前可能需要多长时间。, 此参数可防止dead/failed requests的请求
  - Capacity: This parameter defines how many asynchronous requests may be in progress at the same time. 
              此参数定义可能同时有多少异步请求正在进行中。
              Even though the async I/O approach leads typically to much better throughput, the operator can still be the bottleneck in the streaming application. 
              尽管异步I/O方法通常会带来更好的吞吐量，但operator仍然可能成为流应用程序的瓶颈。
              Limiting the number of concurrent requests ensures that the operator will not accumulate an ever-growing backlog of pending requests, but that it 
              will trigger backpressure once the capacity is exhausted.
              限制并发请求的数量可确保操作员不会累积不断增加的待处理请求积压，但一旦容量耗尽就会触发反压。
              
### 二, Timeout Handling
When an async I/O request times out, by default an exception is thrown and job is restarted. If you want to handle timeouts, you can override the AsyncFunction#timeout method.
当异步I/O请求超时时，默认情况下会引发异常并重新启动作业。,如果要处理超时，可以覆盖AsyncFunction＃timeout方法。

### 三, Order of Results

- **order async i/o模式下,对性能的影响**
  - 在async i/o中, order和unorder都是异步请求, order只是会先缓存一定的结果, 然后一起发送过去, 两者的延迟差距几乎可以忽略.
  - 主要的差距是两者的checkpointing, order模式会在checkpointing中引入一些额外的延迟和一些开销，因为与无序模式相比，records或results在检查点状态下保持更长的时间。

- The concurrent requests issued by the AsyncFunction frequently complete in some undefined order, based on which request finished first. 
  AsyncFunction发出的并发请求经常以某种未定义的顺序完成，具体取决于首先完成的请求.

- To control in which order the resulting records are emitted, Flink offers two modes:
  为了控制发出结果记录的顺序，Flink提供了两种模式：
  - Unordered: Result records are emitted as soon as the asynchronous request finishes. 
           异步请求完成后立即发出结果记录。(谁先完成,就立即发送次记录,有序需要进行一定的缓存)
           The order of the records in the stream is different after the async I/O operator than before.
           在经过异步I/O operator之后，流中记录的顺序与之前不同。
           This mode has the lowest latency and lowest overhead, when used with processing time as the basic time characteristic.
           当使用**processing time**作为基本时间特性时，此模式具有最低延迟和最低开销。 
           Use AsyncDataStream.unorderedWait(...) for this mode.

  - Ordered: In that case, the stream order is preserved.
             在这种模式下，保留流顺序。
         Result records are emitted in the same order as the asynchronous requests are triggered (the order of the operators input records).
         结果记录的发出顺序与触发异步请求的顺序相同（operators输入记录的顺序）。 
         To achieve that, the operator buffers a result record until all its preceding records are emitted (or timed out).
         为此，运算符缓冲结果记录，直到发出所有前面的记录（或超时） 
         This usually introduces some amount of extra latency and some overhead in checkpointing, because records or results are maintained in the checkpointed state for a longer time, compared to the unordered mode. 
         这通常会在checkpointing中引入一些额外的延迟和一些开销，因为与无序模式相比，records或results在检查点状态下保持更长的时间。
         Use AsyncDataStream.orderedWait(...) for this mode.
         
### 四, Event Time
- 注: Event time下的异步I/O
  - unOrder: **这意味着在存在watermarks的情况下(Ingestion Time和event time有水印)，unOrder模式引入了与Order模式相同的延迟和管理开销.**
  - order: **与processing time相比，开销没有显着变化。**

- When the streaming application works with event time, watermarks will be handled correctly by the asynchronous I/O operator.  
  当流应用程序与event time一起工作时，异步I/O operator将正确处理水印。

- That means concretely the following for the two order modes:
  - Unordered: Watermarks do not overtake records and vice versa, meaning watermarks establish an order boundary. 
           Watermarks不会超过记录，反之亦然，这意味着Watermarks建立了顺序的边界。
           Records are emitted unordered only between watermarks.
           Records仅在watermarks之间无序发出。 
           A record occurring after a certain watermark will be emitted only after that watermark was emitted.
           在某个watermark之后发生的record只会在该watermark发出之后才会发出。 
           The watermark in turn will be emitted only after all result records from inputs before that watermark were emitted.
           watermark只能在watermark发出之前输入的所有结果记录之后发出。
           
           That means that in the presence of watermarks, the unordered mode introduces some of the same latency and management overhead as the ordered mode does.
           **这意味着在存在watermarks的情况下(Ingestion Time和event time才会有水印)，无序模式引入了与排序模式相同的延迟和管理开销.**
           The amount of that overhead depends on the watermark frequency.

  - Ordered: Order of watermarks an records is preserved, just like order between records is preserved. 
             There is no significant change in overhead, compared to working with processing time.
             **与processing time相比，开销没有显着变化。**

- Please recall that Ingestion Time is a special case of event time with automatically generated watermarks that are based on the sources processing time.
  请记住，Ingestion Time是event time的一种特殊情况，其中automatically generated watermarks基于sources processing time。
  
### 五. Fault Tolerance Guarantees
- The asynchronous I/O operator offers full exactly-once fault tolerance guarantees. 
  异步I/O operator 提供exactly-once的容错保证
- It stores the records for in-flight asynchronous requests in checkpoints and restores/re-triggers the requests when recovering from a failure.
  它将运行中的异步请求的记录存储在检查点中，并在故障恢复时恢复/重新触发请求。

### 六. Implementation Tips
For implementations with Futures that have an Executor (or ExecutionContext in Scala) for callbacks, 
we suggests to use a DirectExecutor, because the callback typically does minimal work, and a DirectExecutor avoids an additional thread-to-thread handover overhead. 
我们建议使用DirectExecutor，因为回调通常只做最小的工作，而DirectExecutor避免了额外的线程到线程的切换开销。
The callback typically only hands the result to the ResultFuture, which adds it to the output buffer. From there, the heavy logic that includes record emission and interaction with the checkpoint bookkeeping happens in a dedicated thread-pool anyways.

A DirectExecutor can be obtained via org.apache.flink.runtime.concurrent.Executors.directExecutor() or com.google.common.util.concurrent.MoreExecutors.directExecutor().

