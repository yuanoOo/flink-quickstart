# WaterMark
- http://aitozi.com/flink-watermark.html，在该文章中提到两种情况：情况一: late element，情况二：乱序。不用纠结，其实就是一种情况
  ，只是看晚到的元素如何触发其所属的窗口，可能其所属的窗口早就已经触发过了，其自然会被抛弃。或者其所属的窗口还没被触发（该窗口先到的事件
  的watermark不够触发该窗口），而这个晚到的元素的Watermark够了（Watermark > window end time），这个窗口就会被触发。
- waterMark，latency，checkpoint这三者实现方式都是上游节点逐步广播消息给下游节点来处理的行为（都是在流中插入一种特殊的数据结构来做处理）

## 测试验证
- 将程序并行度设置为1，否则每一个并行度都会打印一次当前的WaterMark，
  即向下游发送一次WaterMark，然后下游再按照最大的进行更新，例如在下
  面的例子中，程序运行最初，每个并行度都会打印1970这个最初的WaterMark
 ，不利于测试验证，理解为什么会这样打印就好了。
  ```
    Map -> Timestamps/Watermarks (1/4): timestamp:1,timestamp: 2016-04-27 19:34:22.000,currentMaxTimestamp: 2016-04-27 19:34:22.000,Watermark @ -10001970-01-01 07:59:59.000
    Map -> Timestamps/Watermarks (2/4): timestamp:1,timestamp: 2016-04-27 19:34:23.000,currentMaxTimestamp: 2016-04-27 19:34:23.000,Watermark @ -10001970-01-01 07:59:59.000
    Map -> Timestamps/Watermarks (3/4): timestamp:1,timestamp: 2016-04-27 19:34:24.000,currentMaxTimestamp: 2016-04-27 19:34:24.000,Watermark @ -10001970-01-01 07:59:59.000
  ```
  
  
## 理解WaterMark
- 1、在Flink中，window是按照自然时间进行划分的，如果window大小是3秒，那么1分钟内会把window划分为如下的形式:
    ```
    [00:00:00,00:00:03)
    [00:00:03,00:00:06)
    ...
    [00:00:57,00:01:00)
    ```
    如果window大小是10秒，则window会被分为如下的形式：当然还有一个offset值可以控制window的起始值不是整点。
    ```
    [00:00:00,00:00:10)
    [00:00:10,00:00:20)
    ...
    [00:00:50,00:01:00)
    ```
- 2、输入的数据中，根据自身的Event Time，将数据划分到不同的window中，如果window中有数据，
     则当watermark时间 >= window maxTimestamp(window endTime - 1)时, 就符合了window触发的条件了。
     
- 3、**最终决定window触发，还是由数据本身的Event Time所属的window中的window endTime决定**。
     > 理解：晚到的元素所属的窗口，可能由于缺少触发的条件（即：已经进入该窗口的元素，也就是说未迟到
            的元素（或者说先到的）的WaterMark可能小于Window的endTime），迟迟无法完成触发，此时来了
            一个晚到的元素，由于此时的WaterMark已经被后面的元素驱动上升到足够触发该窗口的高度。因此
            当该晚到的元素携带一个此时的WaterMark进入该窗口，该窗口就被触发了。该窗口再有晚到的元素，
            就会被丢弃。
            
- 4、晚到的元素如何触发窗口：晚到的元素只会触发自己所属的窗口。
     **晚到的元素所属的窗口，可能由于缺少触发的条件（即：已经进入该窗口的元素，也就是说未迟到
     的元素（或者说先到的）的WaterMark小于Window的endTime），迟迟无法完成触发，此时来了
     一个晚到的元素，由于此时的WaterMark已经被后面的元素驱动上升到足够触发该窗口的高度。因此
     当该晚到的元素携带一个此时的WaterMark进入该窗口，该窗口就被触发了。该窗口再有晚到的元素，
     就会被丢弃。**
     > 也就是说如果晚到的元素所属的窗口，在晚到的元素之前就已经被触发了（先前元素的WaterMark已经够触发该窗口了），
       那么自然该晚到的元素就会被丢弃。

- 5、由于我们是根据maxTimestamp定义的WaterMark：````override def getCurrentWatermark: Watermark = new Watermark(maxTimestamp - allowLateness * 1000L)```，
     所以：
     WaterMark只会被后面的元素驱动不断增大，不会减少，并且下游可能会收到上游subTask发的多个WaterMark，而只有当上游发来的WaterMark
     大于下游的WaterMark时，下游才会更新Watermark，也就是说下游总会持有自己所接受到最大的WaterMark。
     ```scala
       class BoundedLatenessWatermarkAssigner(allowLateness: Int) extends AssignerWithPeriodicWatermarks[ComputeResult] {
         private var maxTimestamp = -1L
         
         // 我们是根据maxTimestamp定义的WaterMark，因此WaterMark只会被后面的元素驱动不断增大，不会减少。
         override def getCurrentWatermark: Watermark = {
           new Watermark(maxTimestamp - allowLateness * 1000L)
         }
         
         override def extractTimestamp(t: ComputeResult, l: Long): Long = {
           val timestamp = t.metaData(FIELD_TIMESTAMP_INTERNAL).asInstanceOf[Long]
           if (timestamp > maxTimestamp) {
             maxTimestamp = timestamp
           }
           timestamp
         }
       }
     ```
     
- 6、Flink中计算窗口时间的单位为ms，即时间戳, 也就是说可能就差那一毫秒，Window就被触发了。

- 7、**allowLatency**
  > 默认情况下当watermark涨过了window的endtime之后，再有属于该窗口的数据到来的时候该数据会被丢弃，
    设置了allowLatency这个值之后，也就是定义了数据在watermark涨过window.endtime但是又在allowlatency
    之前到达的话仍旧会被加到对应的窗口去。会使得窗口再次被触发。Flink会保存窗口的状态直到allow latenness超期。


## 相关源码
- 1、到EventTimeTrigger的onElement中看看：EventTimeTrigger中当ctx.getCurrentWatermark >= window.maxTimestamp时立刻触发窗口计算。
```
public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
	if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
		// if the watermark is already past the window fire immediately
		return TriggerResult.FIRE;
	} else {
		ctx.registerEventTimeTimer(window.maxTimestamp());
		return TriggerResult.CONTINUE;
	}
}
```
window.maxTimestamp = 窗口结束时间 - 1，flink时间窗口的单位为ms，也就是时间戳，也就是说就差一毫秒，也不会触发窗口。
```
	public long maxTimestamp() {
		return end - 1;
	}
```
然后到调用Evictor的地方看看：没有内容是不会触发计算的
```
				TriggerResult triggerResult = triggerContext.onElement(element);

				if (triggerResult.isFire()) {
					Iterable<StreamRecord<IN>> contents = evictingWindowState.get();
					if (contents == null) {
						// if we have no state, there is nothing to do
						continue;
					}
					emitWindowContents(actualWindow, contents, evictingWindowState);
				}
```