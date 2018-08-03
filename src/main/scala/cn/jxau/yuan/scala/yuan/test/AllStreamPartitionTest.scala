package cn.jxau.yuan.scala.yuan.test

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

/**
  * @author zhaomingyuan
  * @date 18-8-3
  * @time 下午2:19
  *
  * 据测试,只要利用变量进行分组,就是全分组
  */
object AllStreamPartitionTest {

    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        val ele = env.fromElements("123", "445", "789")

        ele.filter(e => {
            println("你好呀,狗蛋", e)
            1 == 1
        }).setParallelism(2).print().setParallelism(1)

        ele.filter(e => {
            println("小鸡炖蘑菇", e)
            1 == 1
        }).setParallelism(2).print().setParallelism(1)

        env.execute("local")
    }
}
