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
The concurrent requests issued by the AsyncFunction frequently complete in some undefined order, based on which request finished first. To control in which order the resulting records are emitted, Flink offers two modes:

Unordered: Result records are emitted as soon as the asynchronous request finishes. The order of the records in the stream is different after the async I/O operator than before. This mode has the lowest latency and lowest overhead, when used with processing time as the basic time characteristic. Use AsyncDataStream.unorderedWait(...) for this mode.

Ordered: In that case, the stream order is preserved. Result records are emitted in the same order as the asynchronous requests are triggered (the order of the operators input records). To achieve that, the operator buffers a result record until all its preceding records are emitted (or timed out). This usually introduces some amount of extra latency and some overhead in checkpointing, because records or results are maintained in the checkpointed state for a longer time, compared to the unordered mode. Use AsyncDataStream.orderedWait(...) for this mode.
