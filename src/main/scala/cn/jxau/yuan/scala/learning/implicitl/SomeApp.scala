package cn.jxau.yuan.scala.learning.implicitl

/**
  * @author zhaomingyuan
  * @date 18-8-10
  * @time 下午5:24
  */
object SomeApp  extends App {

    class Setting(config: Config){
        def host: String = config.getString
    }

    implicit val setting = new Setting(new Config)

//    def startServer()(implicit setting: Setting): Unit = {
//        val host = setting.host
//        println(s"server listening on $host")
//    }

    def startServer(): Unit = {
        // 隐式类型转换
        val host = implicitly[Setting].host
        println(s"server listening on $host")
    }

    startServer()
}


class Config(){

    def getString: String = "www.google.com"
}