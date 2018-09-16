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

- 2、Scala链式数据结构选择：
选择使用Seq时
  - 若需要索引下标功能，优先考虑选择Vector
  - 若需要Mutable的集合，则选择ArrayBuffer：
  - 若要选择Linear集合，优先选择List
  - 若需要Mutable的集合，则选择ListBuffer。
  - 若需要快速、通用、不变、带顺序的集合，应优先考虑使用Vector。
  - 如果需要选择通用的可变集合，应优先考虑使用ArrayBuffer。
- 一个原则是：当你对选择集合类型犹疑不定时，就应选择使用Vector。
需要注意的是：当我们创建了一个IndexSeq时，Scala实际上会创建Vector对象：
```scala
1 scala> val x = IndexedSeq(1,2,3) 
2 x: IndexedSeq[Int]  = Vector(1, 2, 3)
```
> Vector很好地平衡了快速的随机选择和快速的随机更新（函数式）操作。Vector是Scala集合库中最灵活的高效集合。

> 当面对一个大的集合，且新元素总是要添加到集合末尾时，就可以选择ArrayBuffer。如果使用的可变集合特性更近似于List这样的线性集合，则考虑使用ListBuffer。

> 如果需要将大量数据添加到集合中，建议选择使用List的prepend操作，将这些数据添加到List头部，最后做一次reverse操作。

- 3、异步：Future的使用，在使用Future进行并发处理时，应使用回调的方式，而非阻塞。
```scala
// 避免
val f = Future {
 //executing long time
}
val result = Await.result(f, 5 second)

//推荐
val f = Future {
 //executing long time
}
f.onComplete {
 case Success(result) => //handle result
 case Failure(e) => e.printStackTrace
}
```