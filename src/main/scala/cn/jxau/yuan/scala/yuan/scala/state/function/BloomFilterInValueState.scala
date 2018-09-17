package cn.jxau.yuan.scala.yuan.scala.state.function

import com.google.common.hash._
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.functions.RichFilterFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer
import org.apache.flink.configuration.Configuration
import org.slf4j.LoggerFactory

/**
  * @author zhaomingyuan
  * @date 18-9-10
  * @time 下午4:25
  */
class BloomFilterInValueState extends RichFilterFunction[String] {

    private val LOG = LoggerFactory.getLogger(classOf[BloomFilterInValueState])
    private val BLOOM_FPP: Double = 0.0001d
    private val EXPECTED_INSERTIONS = 1024 * 32
    private def bloomFilter: BloomFilter[CharSequence] = BloomFilter.create(Funnels.stringFunnel(), EXPECTED_INSERTIONS, BLOOM_FPP)

    private var localBloom: BloomFilter[CharSequence] = _
    private var sum: ValueState[BloomFilter[CharSequence]] = _
    private var point: ValueState[Long] = _
    private var longPoint: Long = _

    override def filter(value: String): Boolean = {
        if (sum.value() == null) {
            sum.update(bloomFilter)
            localBloom = sum.value()
            longPoint = point.value()
        }

        if (!localBloom.mightContain(value)) {
            localBloom.put(value)
            longPoint += 1
            println(value + "===> false")
        }

        if (longPoint >= EXPECTED_INSERTIONS) rotaBloomFilter()

        !sum.value().mightContain(value)
    }

    override def open(parameters: Configuration): Unit = {
        val config = new ExecutionConfig
        val serializer = new KryoSerializer[BloomFilter[CharSequence]](classOf[BloomFilter[CharSequence]], config)
        val descriptor = new ValueStateDescriptor[BloomFilter[CharSequence]]("bloom-filter", serializer)

        sum = getRuntimeContext.getState(descriptor)

        val pointDescriptor = new ValueStateDescriptor[Long]("point", new TypeHint[Long]() {}.getTypeInfo)
        point = getRuntimeContext.getState(pointDescriptor)
    }

    def rotaBloomFilter(): Unit = {
        sum.update(bloomFilter)
        localBloom = sum.value()
        println(localBloom.toString)
        LOG.info("init bloom filter...")

        point.update(0)
        longPoint = point.value()
        LOG.info("init point...")
    }
}
