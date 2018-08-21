/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kudu.internal;

import kudu.batch.KuduOutputFormat;
import org.apache.flink.api.java.tuple.Tuple;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KuduUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KuduUtils.class);

    /**
     * Returns a new client connenection from a master address
     *
     * @param masterAddress The kudu master address
     * @return a KuduClient
     */
    public static KuduClient newClient(String masterAddress) {
        return new KuduClient.KuduClientBuilder(masterAddress).build();
    }

    /**
     * Returns a Kudu Table given a table name and a client
     *
     * @param tableName Name of the table
     * @param client    Kudu Client
     * @return a Kudu table instance, or null if it does not exist
     */
    public static KuduTable getTable(String tableName, KuduClient client) {
        try {
            if (client.tableExists(tableName)) {
                return client.openTable(tableName);
            }
            else {
                LOG.warn("Trying to open a non existing table: {}", tableName);
                return null;
            }
        } catch (KuduException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Inserts a Flink tuple as a record in a Kudu table
     *
     * @param tuple   Flink tuple
     * @param table   Kudu table
     * @param session Kudu session
     * @return
     */
    public static OperationResponse insertTuple(
            Tuple tuple,
            KuduTable table,
            KuduSession session,
            KuduOutputFormat.Conf.WriteMode mode) {
        Operation insert = mode == KuduOutputFormat.Conf.WriteMode.INSERT
                ? table.newInsert()
                : table.newUpsert();
        table.newUpsert();
        PartialRow row = insert.getRow();

        for (int i = 0; i < table.getSchema().getColumnCount(); i++) {
            ColumnSchema columnSchema = table.getSchema().getColumnByIndex(i);
            Type type = columnSchema.getType();
            switch (type) {
                case BINARY:
                    row.addBinary(i, (byte[]) tuple.getField(i));
                    break;
                case STRING:
                    row.addString(i, tuple.getField(i));
                    break;
                case BOOL:
                    row.addBoolean(i, tuple.getField(i));
                    break;
                case DOUBLE:
                    row.addDouble(i, tuple.getField(i));
                    break;
                case FLOAT:
                    row.addFloat(i, tuple.getField(i));
                    break;
                case INT8:
                    row.addByte(i, tuple.getField(i));
                    break;
                case INT16:
                    row.addShort(i, tuple.getField(i));
                    break;
                case INT32:
                    row.addInt(i, tuple.getField(i));
                    break;
                case INT64:
                case UNIXTIME_MICROS:
                    row.addLong(i, tuple.getField(i));
                    break;
                default:
                    throw new IllegalArgumentException("Illegal var type: " + type);
            }
        }
        try {
            return session.apply(insert);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Inserts a Flink tuple as a record in a Kudu table
     *
     * @param tuple   Flink tuple
     * @param table   Kudu table
     * @param session Kudu session
     * @return
     */
    public static OperationResponse insertKuduTuple(
            KuduTuple tuple,
            KuduTable table,
            KuduSession session,
            KuduOutputFormat.Conf.WriteMode mode) {
        Operation insert = mode == KuduOutputFormat.Conf.WriteMode.INSERT
                ? table.newInsert()
                : table.newUpsert();
        table.newUpsert();
        PartialRow row = insert.getRow();

        for (int i = 0; i < table.getSchema().getColumnCount(); i++) {
            ColumnSchema columnSchema = table.getSchema().getColumnByIndex(i);
            Type type = columnSchema.getType();
            switch (type) {
                case BINARY:
                    row.addBinary(i, (byte[]) tuple.getField(i));
                    break;
                case STRING:
                    row.addString(i, tuple.getField(i));
                    break;
                case BOOL:
                    row.addBoolean(i, tuple.getField(i));
                    break;
                case DOUBLE:
                    row.addDouble(i, tuple.getField(i));
                    break;
                case FLOAT:
                    row.addFloat(i, tuple.getField(i));
                    break;
                case INT8:
                    row.addByte(i, tuple.getField(i));
                    break;
                case INT16:
                    row.addShort(i, tuple.getField(i));
                    break;
                case INT32:
                    row.addInt(i, tuple.getField(i));
                    break;
                case INT64:
                case UNIXTIME_MICROS:
                    row.addLong(i, tuple.getField(i));
                    break;
                default:
                    throw new IllegalArgumentException("Illegal var type: " + type);
            }
        }
        try {
            return session.apply(insert);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * It creates a Kudu session using a KuduClient instance
     *
     * @param client KuduClient
     * @return Kudu session
     */
    public static KuduSession createSession(KuduClient client) {
        return client.newSession();
    }

    /**
     * Converts a Kudu RowResult into a Flink Tuple.
     *
     * @param row a Kudu record
     * @return Flink Tuple
     */
    public static <OUT extends Tuple> OUT rowResultToTuple(RowResult row) {
        Schema columnProjection = row.getColumnProjection();
        int columns = columnProjection.getColumnCount();

        Tuple tuple = null;

        try {
            tuple = Tuple.getTupleClass(columns).newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

        for (int i = 0; i < columns; i++) {
            Type type = row.getColumnType(i);
            switch (type) {
                case BINARY:
                    tuple.setField(row.getBinary(i), i);
                    break;
                case STRING:
                    tuple.setField(row.getString(i), i);
                    break;
                case BOOL:
                    tuple.setField(row.getBoolean(i), i);
                    break;
                case DOUBLE:
                    tuple.setField(row.getDouble(i), i);
                    break;
                case FLOAT:
                    tuple.setField(row.getFloat(i), i);
                    break;
                case INT8:
                    tuple.setField(row.getByte(i), i);
                    break;
                case INT16:
                    tuple.setField(row.getShort(i), i);
                    break;
                case INT32:
                    tuple.setField(row.getInt(i), i);
                    break;
                case INT64:
                case UNIXTIME_MICROS:
                    tuple.setField(row.getLong(i), i);
                    break;
                default:
                    throw new IllegalArgumentException("Illegal var type: " + type);
            }
        }
        return (OUT) tuple;
    }
}