package suishen.flink.common.hbase;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * HBaseSinkBase is the common abstract class of {@link HBasePojoSink}, {@link HBaseTupleSink}, {@link HBaseRowSink} and
 * {@link HBaseScalaProductSink}.
 *
 * @param <IN> Type of the elements emitted by this sink
 */
public abstract class HBaseSinkBase<IN> extends RichSinkFunction<IN> implements CheckpointedFunction {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * The timer that triggers periodic flush to HBase.
     */
    private ScheduledThreadPoolExecutor timer;

    /**
     * The lock to safeguard the flush commits.
     */
    private Object lock = null;

    protected Connection connection;
    protected transient Table hTable;

    private final HBaseBuilder builder;

    public HBaseSinkBase(HBaseBuilder builder) {
        this.builder = builder;
        ClosureCleaner.clean(builder, true);
    }

    @VisibleForTesting
    public HBaseSinkBase(Table hTable) {
        this.builder = null;
        this.connection = null;
        this.hTable = hTable;
    }

    @Override
    public void open(Configuration configuration) throws Exception {
        lock = new Object();
        super.open(configuration);
        if (builder != null) {
            this.connection = builder.buildConnection();
        }
        if (connection != null) {
            this.hTable = builder.buildTable(connection);
        }
        this.timer = new ScheduledThreadPoolExecutor(1);
        if (builder != null && builder.getFlushPeriod() > 0) {
            timer.scheduleAtFixedRate(() -> {
                if (this.hTable != null && this.hTable instanceof HTable) {
                    synchronized (lock) {
                        try {
                            ((HTable) this.hTable).flushCommits();
                        } catch (IOException e) {
                            log.info("Periodic flush commits failed.", e);
                        }
                    }
                }
            }, 0, builder.getFlushPeriod(), TimeUnit.SECONDS);
        }
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        Object obj = extract(value);
        if (obj == null) {
            return;
        } else if (obj instanceof Put) {
            this.hTable.put((Put) obj);
        } else if (obj instanceof Delete) {
            this.hTable.delete((Delete) obj);
        }
    }

    protected abstract Object extract(IN value) throws Exception;

    @Override
    public void close() throws Exception {
        super.close();
        try {
            if (this.hTable != null) {
                this.hTable.close();
            }
        } catch (Throwable t) {
            log.error("Error while closing HBase table.", t);
        }
        try {
            if (this.connection != null) {
                this.connection.close();
            }
        } catch (Throwable t) {
            log.error("Error while closing HBase connection.", t);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        if (this.hTable != null && this.hTable instanceof HTable) {
            synchronized (lock) {
                ((HTable) this.hTable).flushCommits();
            }
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
    }

    protected byte[] getByteArray(Object obj, Class clazz) {
        if (obj == null) {
            return new byte[0];
        }
        if (clazz == Integer.class) {
            return Bytes.toBytes((Integer) obj);
        } else if (clazz == Long.class) {
            return Bytes.toBytes((Long) obj);
        } else if (clazz == String.class) {
            return Bytes.toBytes((String) obj);
        } else if (clazz == Byte.class) {
            return Bytes.toBytes((Byte) obj);
        } else if (clazz == Short.class) {
            return Bytes.toBytes((Short) obj);
        } else if (clazz == Float.class) {
            return Bytes.toBytes((Float) obj);
        } else if (clazz == Double.class) {
            return Bytes.toBytes((Double) obj);
        } else if (clazz == Character.class) {
            return Bytes.toBytes((Character) obj);
        } else if (clazz == Void.class) {
            return new byte[0];
        } else {
            return Bytes.toBytes(obj.toString());
        }
    }
}
