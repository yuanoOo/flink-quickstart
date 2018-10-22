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
        env.setParallelism(1)
        val ele = env.fromElements("123", "445", "789").setParallelism(1)
        val ele1 = env.fromElements("000", "111", "222").setParallelism(1)

        val union = ele.union(ele1).filter(e => 1 == 1)

        union.filter(e => {
            1 == 1
        }).map(e => e + "streming-1" ).setParallelism(1).print().setParallelism(1)

        union.filter(e => {
            1 == 1
        }).map(e => e + "streming-2" ).setParallelism(1).print().setParallelism(1)

        println(env.getExecutionPlan)
        env.execute("local")
    }
}
