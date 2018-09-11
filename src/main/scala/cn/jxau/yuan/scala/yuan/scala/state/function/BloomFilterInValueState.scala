package cn.jxau.yuan.scala.yuan.scala.state.function

import com.google.common.base.Charsets
import com.google.common.hash.{BloomFilter, Funnel, Sink}
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.configuration.Configuration
import org.slf4j.LoggerFactory

/**
  * @author zhaomingyuan
  * @date 18-9-10
  * @time 下午4:25
  */
class BloomFilterInValueState extends RichFilterFunction[Tuple2[Long, Long]]{

    private val LOG = LoggerFactory.getLogger(classOf[BloomFilterInValueState])
    private val BLOOM_FPP = 0.0000001d
    private val EXPECTED_INSERTIONS = 1024 * 1024 * 32

    private var sum: ValueState[BloomFilter[String]] = _

    override def filter(value: Tuple2[Long, Long]): Boolean = {
        val bloomFilter: BloomFilter[String] = BloomFilter.create(new Funnel[String] {
            override def funnel(from: String, into: Sink): Unit = into.putString(from, Charsets.UTF_8)
        }, EXPECTED_INSERTIONS, BLOOM_FPP)
        if (sum.value() == null)
            sum.update(bloomFilter)

        !sum.value().mightContain(value._1.toString)
    }

    override def open(parameters: Configuration): Unit = {
        val config = new ExecutionConfig
        val serializer = new KryoSerializer[BloomFilter[String]](classOf[BloomFilter[String]], config)


        val descriptor = new ValueStateDescriptor[BloomFilter[String]]("bloom-filter", serializer)

        sum = getRuntimeContext.getState(descriptor)
    }
}
