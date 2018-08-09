### 

- 1. org.apache.flink.api.common.functions.RuntimeContext
  - A RuntimeContext contains information about the context in which functions are executed. 
    RuntimeContext包含有关执行函数的上下文的信息。
    
    Each parallel instance of the function will have a context through which it can access static contextual information (such as 
    the current parallelism) and other constructs like accumulators and broadcast variables.
    函数的每个并行实例都有一个上下文，通过它可以访问静态上下文信息（例如当前并行性）和其他结构体，如累加器和广播变量。
   
    A function can, during runtime, obtain the RuntimeContext via a call to {@link AbstractRichFunction#getRuntimeContext()}.
    在运行时，函数可以通过调用{@link AbstractRichFunction＃getRuntimeContext（）}来获取RuntimeContext。