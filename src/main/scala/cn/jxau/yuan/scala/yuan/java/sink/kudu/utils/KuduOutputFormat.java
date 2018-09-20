package cn.jxau.yuan.scala.yuan.java.sink.kudu.utils;


import kuduNoTuple.Utils.RowSerializable;
import kuduNoTuple.Utils.TableUtils;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


public class KuduOutputFormat extends RichOutputFormat<RowSerializable> implements CheckpointedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);
    private transient KuduClient client;
    private transient KuduSession session;
    private transient KuduTable table;
    private String[] fieldsNames;
    /**
     * The lock to safeguard the flush commits.
     */
    private final transient Object lock = new Object();
    private ScheduledThreadPoolExecutor timer;
    private String host;
    private String tableName;
    private int flushPeriodSec;


    public KuduOutputFormat(String host, String tableName, int flushPeriodSec) {

        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"host\" not valid (null or empty)");

        } else if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("ERROR: Param \"tableName\" not valid (null or empty)");
        }

        this.tableName = tableName;
        this.host = host;
        this.flushPeriodSec = flushPeriodSec;
    }


    @Override
    public void configure(Configuration configuration) {

    }

    @Override
    public void open(int i, int i1) throws IOException {
        this.client = new KuduClient.KuduClientBuilder(host).build();
        this.table = client.openTable(tableName);
        this.session = this.client.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(100000);

        if (fieldsNames == null || fieldsNames.length == 0) {
            fieldsNames = getNamesOfColumns(table);
        }

        this.timer = new ScheduledThreadPoolExecutor(1);
        if (client != null && table != null && session != null) {
            timer.scheduleAtFixedRate(() -> {
                synchronized (lock) {
                    try {
                        session.flush();
                    } catch (IOException e) {
                        LOG.info("Periodic flush commits failed.", e);
                    }
                }
            }, 0, flushPeriodSec, TimeUnit.SECONDS);
        }
    }


    @Override
    public void writeRecord(RowSerializable row) throws IOException {
        Insert insert = table.newInsert();
        for (int index = 0; index < row.productArity(); index++) {
            Type type = TableUtils.mapToType(row.productElement(index).getClass());
            PartialRow partialRow = insert.getRow();
            TableUtils.valueToRow(partialRow, type, fieldsNames[index], row.productElement(index));
        }
        session.apply(insert);
    }


    private String[] getNamesOfColumns(KuduTable table) {
        List<ColumnSchema> columns = table.getSchema().getColumns();
        List<String> columnsNames = new ArrayList<>(); //  List of column names
        for (ColumnSchema schema : columns) {
            columnsNames.add(schema.getName());
        }
        String[] array = new String[columnsNames.size()];
        array = columnsNames.toArray(array);
        return array;
    }


    @Override
    public void snapshotState(FunctionSnapshotContext context) throws KuduException {
        if (client != null && table != null && session != null) {
            session.flush();
        }
    }


    @Override
    public void initializeState(FunctionInitializationContext context) {

    }

    @Override
    public void close() throws IOException {
        if (client != null && table != null && session != null) {
            session.flush();
            this.session.close();
            this.client.close();
        }
    }

}

