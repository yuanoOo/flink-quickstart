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


### TimeWindow
- 1、注意TimeWindow的开始和结束时间是左开右闭：
     1:00:00.000 - 1:59:59.999 || max ===> 2018-09-15 20:11:59

- 2、时间窗口偏移量
```scala
input
    .keyBy(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(-8)))
    .<windowed transformation>(<window function>)
```
As shown in the last example, tumbling window assigners also take an optional offset parameter 
that can be used to change the alignment of windows. For example, without offsets hourly tumbling 
windows are aligned with epoch, that is you will get windows such as 
1:00:00.000 - 1:59:59.999, 2:00:00.000 - 2:59:59.999 and so on. 
If you want to change that you can give an offset. 
With an offset of 15 minutes you would, for example, get 
1:15:00.000 - 2:14:59.999, 2:15:00.000 - 3:14:59.999 etc. 
An important use case for offsets is to adjust windows to timezones other than UTC-0. For example, 
in China you would have to specify an offset of Time.hours(-8).
如上一个示例所示，翻滚窗口分配器还采用可选的偏移参数，可用于更改窗口的对齐方式。
例如，没有偏移每小时翻滚窗口与时期对齐，也就是说你会得到1：00：00.000-1：59：59.999,2：00：00.000 - 2：59：59.999等窗口。
如果你想改变它，你可以给出一个偏移量。如果偏移15分钟，你会得到1：15：00.000 - 2：14：59.999,2：15：00.000 - 3：14：59.999等。
偏移的一个重要用例是调整窗口到时区不是UTC-0。例如，在中国，您必须指定Time.hours（-8）的偏移量。

- 3、一个ProcessTime时间窗口日志打印示例
```
      timeWindow(Time.minutes(1)) && ProcessTime

            now ===> 2018-09-15 20:11:02
            start ===> 2018-09-15 20:10:00
            end ===> 2018-09-15 20:11:00
            max ===> 2018-09-15 20:10:59
            TimeWindow{start=1537013400000, end=1537013460000}
            
            now ===> 2018-09-15 20:12:00
            start ===> 2018-09-15 20:11:00
            end ===> 2018-09-15 20:12:00
            max ===> 2018-09-15 20:11:59
            TimeWindow{start=1537013460000, end=1537013520000}
            
            now ===> 2018-09-15 20:13:00
            start ===> 2018-09-15 20:12:00
            end ===> 2018-09-15 20:13:00
            max ===> 2018-09-15 20:12:59
            TimeWindow{start=1537013520000, end=1537013580000}
```
