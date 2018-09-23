# WaterMark

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