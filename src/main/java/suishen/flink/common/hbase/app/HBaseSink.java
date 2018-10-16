package suishen.flink.common.hbase.app;

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import suishen.flink.common.hbase.HBaseBuilder;
import suishen.flink.common.hbase.HBaseTableOperator;

import java.io.IOException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author zhaomingyuan
 * @date 18-10-11
 * @time 下午5:12
 */
public class HBaseSink extends RichSinkFunction<HBaseTableOperator> implements CheckpointedFunction {
    protected final Logger log = LoggerFactory.getLogger(getClass());

    /**
     * The timer that triggers periodic flush to HBase.
     */
    private ScheduledThreadPoolExecutor timer;

    /**
     * The lock to safeguard the flush commits.
     */
    private Object lock = null;

    private Connection connection;
    private transient Table hTable;

    private final HBaseBuilder builder;

    public HBaseSink(HBaseBuilder builder) {
        this.builder = builder;
        ClosureCleaner.clean(builder, true);
    }

    public HBaseSink(Table hTable) {
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
    public void invoke(HBaseTableOperator value, SinkFunction.Context context) {
        if (value.getHBaseTableIncrement() == null){
        }
    }

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

}
