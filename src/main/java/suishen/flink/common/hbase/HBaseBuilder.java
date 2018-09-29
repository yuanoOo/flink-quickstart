package suishen.flink.common.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * This class is used to configure a {@link Connection} and a {@link Table} after deployment.
 * The connection represents the connection that will be established to HBase.
 * The table represents a table can be manipulated in the hbase.
 */
public class HBaseBuilder implements Serializable {

    // Configuration is not Serializable
    private Map<String, String> configurationMap = new HashMap<>();

    private String tableName;
    private boolean bufferEnabled = false;
    private boolean clusterKeyConfigured = false;
    private int flushPeriod = -1;
    private long bufferSize = -1;

    public HBaseBuilder withClusterKey(String clusterKey) throws IOException {
        mergeClusterkeyToConfiguration(clusterKey);
        clusterKeyConfigured = true;
        return this;
    }

    public HBaseBuilder withTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public HBaseBuilder enableBuffer(boolean bufferEnabled) {
        this.bufferEnabled = bufferEnabled;
        return this;
    }

    public int getFlushPeriod() {
        return flushPeriod;
    }

    public void setFlushPeriod(int flushPeriod) {
        this.flushPeriod = flushPeriod;
    }

    public void setBufferSize(long bufferSize) {
        this.bufferSize = bufferSize;
    }

    public HBaseBuilder addProperty(String key, String value) {
        configurationMap.put(key, value);
        return this;
    }

    public boolean isClusterKeyConfigured() {
        return clusterKeyConfigured;
    }

    public String getTableName() {
        return tableName;
    }

    public Connection buildConnection() throws IOException {
        Configuration configuration = new Configuration();
        for (String key : configurationMap.keySet()) {
            configuration.set(key, configurationMap.get(key));
        }
        return ConnectionFactory.createConnection(configuration);
    }

    public Table buildTable(Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));

        if (table instanceof HTable && bufferEnabled) {
            HTable hTable = (HTable) table;
            hTable.setAutoFlush(false, false);
            if (bufferSize > 0) {
                hTable.setWriteBufferSize(bufferSize);
            }
        }
        Admin admin = connection.getAdmin();
        try {
            if (!admin.isTableAvailable(TableName.valueOf(this.tableName))) {
                throw new IOException("Table is not available.");
            }
        } finally {
            try {
                if (admin != null) {
                    admin.close();
                }
            } catch (Throwable ignored) {

            }
        }
        return table;
    }

    private void mergeClusterkeyToConfiguration(String clusterKey)
            throws IOException {
        if (clusterKey == null) {
            throw new IOException("ClusterKey is null.");
        }
        String[] segments = clusterKey.split(":");
        if (segments.length > 3) {
            throw new IOException("ClusterKey:[" + clusterKey + "] is illegal.");
        }
        if (segments.length > 0) {
            configurationMap.put(HConstants.ZOOKEEPER_QUORUM, segments[0]);
        }
        if (segments.length > 1) {
            configurationMap.put(HConstants.ZOOKEEPER_CLIENT_PORT, segments[1]);
        }
        if (segments.length > 2) {
            configurationMap.put(HConstants.ZOOKEEPER_ZNODE_PARENT, segments[2]);
        }
    }
}
