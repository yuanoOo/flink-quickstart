package cn.jxau.yuan.scala.learning.map2tuple

import scala.collection.JavaConversions.mapAsScalaMap

/**
  * @author zhaomingyuan
  * @date 18-6-15
  * @time 上午10:49
  */
object MapApp {
    def main(args: Array[String]): Unit = {
        val in  = new java.util.Scanner(new java.io.File("/home/yuan/quickstart/src/main/scala/cn/jxau/yuan/scala/learning/zxz.scala"))
        val map : scala.collection.mutable.Map[String, Int] = new java.util.TreeMap[String, Int]

        while (in.hasNext()) {
            val str = in.next()
            map(str) = map.getOrElse(str, 0) + 1
        }

        println(map.mkString(", "))
    }
}
