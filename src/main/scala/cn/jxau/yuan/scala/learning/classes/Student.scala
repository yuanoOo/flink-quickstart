package cn.jxau.yuan.scala.learning.classes

/**
  * @author zhaomingyuan
  * @date 18-7-3
  * @time 下午4:46
  */

object Student{
    val sex = "man"

    def apply(name: String, age: Int): Student = new Student(name, sex, age)
}

case class Student(name: String, sex: String, age: Int)