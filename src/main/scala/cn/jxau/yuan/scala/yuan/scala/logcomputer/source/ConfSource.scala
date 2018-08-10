package cn.jxau.yuan.scala.yuan.scala.logcomputer.source

import cn.jxau.yuan.scala.yuan.scala.logcomputer.bean.ComputeConf
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import org.slf4j.LoggerFactory

import scala.util.Try
import scalaj.http.Http

/**
  * 配置源:应用于: 需要进行实时规则更新的场景
  *
  * @param confUrl
  */
class ConfSource(confUrl: String) extends SourceFunction[ComputeConf] {
    private val LOG = LoggerFactory.getLogger(classOf[ConfSource])

    @volatile private var isRunning: Boolean = true

    override def run(sourceContext: SourceContext[ComputeConf]): Unit = {
        // scala的json库
        implicit val formats: DefaultFormats.type = DefaultFormats
        // 实时从某个url更新规则:获取json,进行解析
        while (true) {
            Try {
                Http(confUrl).timeout(2000, 60000).asString
            }.toOption match {
                case Some(response) =>
                    response.code match {
                        case 200 =>
                            parse(response.body).extractOpt[ComputeConf] match {
                                case Some(conf) =>
                                    LOG.info("Pulled configuration: {}", response.body)
                                    sourceContext.collect(conf)
                                case None => LOG.warn("Invalid configuration: {}", response.body)
                            }
                        case _ => LOG.warn("Pull configuration failed: {}", response.body)
                    }
                case None => LOG.warn("Failed to invoke config API")
            }
            // 1分钟读取拉取一次规则
            Thread.sleep(60000L)
        }
    }

    override def cancel(): Unit = {
        isRunning = false
    }

}
