package suishen.flink.common.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author zhaomingyuan
 * @date 18-10-11
 * @time 下午4:00
 */
public class HBaseTablePut {
    private byte[] columnFamily;
    public byte[] key;
    private Put put;

    public HBaseTablePut(String cloumnFamily, String rowKey) {
        this.columnFamily = Bytes.toBytes(cloumnFamily);
        this.key = Bytes.toBytes(rowKey);
        put = new Put(key);
    }


    public HBaseTablePut addStringColumn(String qualifier, String value) {
        this.put.addColumn(columnFamily, Bytes.toBytes(qualifier), Bytes.toBytes(value));
        return this;
    }

    public HBaseTablePut addLongColumn(String qualifier, long value) {
        this.put.addColumn(columnFamily, Bytes.toBytes(qualifier), Bytes.toBytes(value));
        return this;
    }

    public HBaseTablePut addShortColumn(String qualifier, short value) {
        this.put.addColumn(columnFamily, Bytes.toBytes(qualifier), Bytes.toBytes(value));
        return this;
    }

    public HBaseTablePut addBoolColumn(String qualifier, boolean value) {
        this.put.addColumn(columnFamily, Bytes.toBytes(qualifier), Bytes.toBytes(value));
        return this;
    }

    public HBaseTablePut addFloatColumn(String qualifier, boolean value) {
        this.put.addColumn(columnFamily, Bytes.toBytes(qualifier), Bytes.toBytes(value));
        return this;
    }

    public HBaseTablePut addIntColumn(String qualifier, int value) {
        this.put.addColumn(columnFamily, Bytes.toBytes(qualifier), Bytes.toBytes(value));
        return this;
    }

    public HBaseTablePut addDoubleColumn(String qualifier, double value) {
        this.put.addColumn(columnFamily, Bytes.toBytes(qualifier), Bytes.toBytes(value));
        return this;
    }

    public HBaseTablePut addByteArrayColumn(String qualifier, byte[] value) {
        this.put.addColumn(columnFamily, Bytes.toBytes(qualifier), value);
        return this;
    }
}
