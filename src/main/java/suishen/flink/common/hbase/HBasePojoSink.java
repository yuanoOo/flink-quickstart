package suishen.flink.common.hbase;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

import java.lang.reflect.Field;

/**
 * Flink Sink to save data into a HBase cluster.
 *
 * @param <IN> Type of the element emitted by this sink
 */
public class HBasePojoSink<IN> extends HBaseSinkBase<IN> {

    private static final long serialVersionUID = 1L;

    private final HBaseTableMapper tableMapper;
    private final String[] fieldNameList;
    private final TypeInformation<IN> typeInfo;
    private transient Field[] fields;
    private transient Field rowKeyField;

    /**
     * The main constructor for creating HBasePojoSink.
     *
     * @param builder     A builder for build HBase connection and handle for communicating with a single HBase table.
     * @param tableMapper The mapping from a Pojo to a HBase table
     * @param typeInfo    TypeInformation of the Pojo
     */
    public HBasePojoSink(HBaseBuilder builder, HBaseTableMapper tableMapper, TypeInformation<IN> typeInfo) {
        super(builder);
        this.tableMapper = tableMapper;
        this.fieldNameList = tableMapper.getKeyList();
        this.typeInfo = typeInfo;
    }

    @VisibleForTesting
    public HBasePojoSink(Table hTable, HBaseTableMapper tableMapper, TypeInformation<IN> typeInfo) {
        super(hTable);
        this.tableMapper = tableMapper;
        this.fieldNameList = tableMapper.getKeyList();
        this.typeInfo = typeInfo;
    }

    @Override
    protected Object extract(IN value) throws Exception {
        byte[] rowKey = HBaseTableMapper.serialize(tableMapper.getRowKeyType(), rowKeyField.get(value));
        Put put = new Put(rowKey);
        for (int i = 0; i < fieldNameList.length; i++) {
            Tuple3<byte[], byte[], TypeInformation<?>> colInfo = tableMapper.getColInfo(fieldNameList[i]);
            put.addColumn(colInfo.f0, colInfo.f1,
                    HBaseTableMapper.serialize(colInfo.f2, fields[i].get(value)));
        }
        return put;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        super.open(configuration);
        Class<IN> clazz = typeInfo.getTypeClass();
        fields = new Field[fieldNameList.length];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = clazz.getDeclaredField(fieldNameList[i]);
            fields[i].setAccessible(true);
        }
        rowKeyField = clazz.getDeclaredField(tableMapper.getRowKey());
        rowKeyField.setAccessible(true);
    }
}
