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

    private var localBloomFilter: BloomFilter[CharSequence] = _
    private var stateBloomFilter: ValueState[BloomFilter[CharSequence]] = _
    private var point: ValueState[Long] = _
    private var bloomFilterInsertion: Long = _

    override def filter(value: String): Boolean = {
        if (stateBloomFilter.value() == null) {
            stateBloomFilter.update(bloomFilter)
            localBloomFilter = stateBloomFilter.value()
            bloomFilterInsertion = point.value()
        }

        val nonExist = noContain(value)
        if (nonExist) {
            localBloomFilter.put(value)
            bloomFilterInsertion += 1
        }

        if (bloomFilterInsertion >= EXPECTED_INSERTIONS) rotateBloomFilter()

        nonExist
    }

    override def open(parameters: Configuration): Unit = {
        val config = new ExecutionConfig
        val serializer = new KryoSerializer[BloomFilter[CharSequence]](classOf[BloomFilter[CharSequence]], config)
        val descriptor = new ValueStateDescriptor[BloomFilter[CharSequence]]("bloom-filter", serializer)

        stateBloomFilter = getRuntimeContext.getState(descriptor)

        val pointDescriptor = new ValueStateDescriptor[Long]("point", new TypeHint[Long]() {}.getTypeInfo)
        point = getRuntimeContext.getState(pointDescriptor)
    }

    def rotateBloomFilter(): Unit = {
        stateBloomFilter.update(bloomFilter)
        localBloomFilter = stateBloomFilter.value()
        println(localBloomFilter.toString)
        LOG.info("rotate bloom filter...")

        point.update(0)
        bloomFilterInsertion = point.value()
    }

    def noContain(value: String): Boolean = !localBloomFilter.mightContain(value)
}
