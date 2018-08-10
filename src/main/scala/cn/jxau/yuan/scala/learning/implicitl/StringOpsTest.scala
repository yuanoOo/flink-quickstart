package cn.jxau.yuan.scala.learning.implicitl

/**
  * @author zhaomingyuan
  * @date 18-8-10
  * @time 下午6:14
  */

object StringOpsTest extends App {
    // 定义打印操作Trait
    trait PrintOps {
        val value: String
        def printWithSeperator(sep: String): Unit = {
            println(value.split("").mkString(sep))
        }
    }

    // 定义针对String的隐式转换方法
    implicit def stringToPrintOps(str: String): PrintOps = new PrintOps {
        override val value: String = str
    }

    // 定义针对Int的隐式转换方法
    implicit def intToPrintOps(i: Int): PrintOps = new PrintOps {
        override val value: String = i.toString
    }

    // String 和 Int 都拥有 printWithSeperator 函数
    "hello,world" printWithSeperator "*"
    1234 printWithSeperator "*"
}