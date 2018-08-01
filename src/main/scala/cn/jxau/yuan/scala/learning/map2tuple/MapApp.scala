package cn.jxau.yuan.scala.learning.map2tuple

import cn.jxau.yuan.scala.learning.classes.Student

import scala.collection.JavaConversions.mapAsScalaMap

/**
  * @author zhaomingyuan
  * @date 18-6-15
  * @time 上午10:49
  */
object MapApp {
    def main(args: Array[String]): Unit = {
        val in  = new java.util.Scanner(new java.io.File("/home/KafkaConsumerTest/quickstart/src/main/scala/cn/jxau/KafkaConsumerTest/scala/learning/map2tuple/MapApp.scala"))
        val map : scala.collection.mutable.Map[String, Int] = new java.util.TreeMap[String, Int]

        while (in hasNext) {
            val str = in.next()
            map(str) = map.getOrElse(str, 0) + 1
        }

        println(map.mkString(", "))
        println("sds" substring  0)

        val student = Student("12121", 1)
        println(student.name)
    }

}