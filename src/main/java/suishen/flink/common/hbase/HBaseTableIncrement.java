package suishen.flink.common.hbase;

import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author zhaomingyuan
 * @date 18-10-11
 * @time 下午4:37
 */
public class HBaseTableIncrement {
    private byte[] columnFamily;
    private byte[] key;
    public Increment increment;

    public HBaseTableIncrement(String cloumnFamily, String rowKey) {
        this.columnFamily = Bytes.toBytes(cloumnFamily);
        this.key = Bytes.toBytes(rowKey);
        increment = new Increment(key);
    }

    public HBaseTableIncrement addLongColumn(String qualifier, long value) {
        this.increment.addColumn(columnFamily, Bytes.toBytes(qualifier), value);
        return this;
    }
}
