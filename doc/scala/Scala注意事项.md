- 1、在scala中，调用带function参数的方法时，function中的参数SourceContext[T]，需要括号括起来
```scala
    env.addSource((context: SourceContext[String]) => context.collect(""))
```
```scala
  def addSource[T: TypeInformation](function: SourceContext[T] => Unit): DataStream[T] = {
    require(function != null, "Function must not be null.")
    val sourceFunction = new SourceFunction[T] {
      val cleanFun = scalaClean(function)
      override def run(ctx: SourceContext[T]) {
        cleanFun(ctx)
      }
      override def cancel() = {}
    }
    addSource(sourceFunction)
  }
```