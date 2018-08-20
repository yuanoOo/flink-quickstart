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

package kudu.batch;

import kudu.internal.KuduUtils;
import kudu.internal.Predicate;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static kudu.internal.KuduUtils.insertTuple;

public class KuduOutputFormat<OUT extends Tuple> implements OutputFormat<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);

    /**
     * Pointer in charge of handling insert operations
     **/
    private KuduTable table;

    /**
     * Kudu client using its driver
     **/
    private KuduClient client;

    /**
     * Kudu configuration
     **/
    private Conf conf;

    /**
     * Session that is stabilised and closed in the open() and close() method, respectively
     **/
    private KuduSession session;

    /**
     * Name of the table Kudu writes in
     **/
    private String tableName;

    public KuduOutputFormat(Conf conf) {
        this.conf = Preconditions.checkNotNull(conf, "Kudu config cannot be null");
    }

    @Override
    public void configure(Configuration parameters) {
        // Check hat client has been properly initialized
        this.client = Preconditions.checkNotNull(KuduUtils.newClient(conf.masterAddress),
                "Invalid Kudu master address");

        // Check that table handler is not null and table exists
        this.table = Preconditions.checkNotNull(KuduUtils.getTable(conf.tableName, this.client),
                "Kudu table cannot be null. Check if your table actually exists");

        this.session = Preconditions.checkNotNull(KuduUtils.createSession(this.client),
                "Kudu session cannot be null");

        // Check that te number of columns in table is at least one
        Preconditions.checkArgument(this.table.getSchema().getColumnCount() > 0,
                "The total number of columns in your Kudu table should be at least 1");
    }

    @Override
    public void open(int taskNumber, int numTasks) throws IOException {

    }

    @Override
    public void writeRecord(OUT tuple) throws IOException {
        OperationResponse op = insertTuple(tuple, this.table, this.session, conf.writeMode);
        if(op.hasRowError()){
            LOG.warn("An error occurred trying to insert a record in Kudu : {} ", op.getRowError());
        }
    }

    @Override
    public void close() throws IOException {
        if (this.session != null) {
            this.session.close();
        }
        if (this.client != null) {
            this.client.close();
        }
    }


    /**
     * A serializable, configuration class that allows to configure a KuduOutputFormat.
     * This class provides a fluent API to ease users to provide configuration in a easy way.
     */
    public static class Conf implements Serializable {
        private String masterAddress;

        private String tableName;

        private List<Predicate> predicates = new ArrayList<>();

        private WriteMode writeMode = WriteMode.INSERT;

        public enum WriteMode {
            INSERT, UPSERT
        }

        private Conf() {
        }

        public String getMasterAddress() {
            return masterAddress;
        }

        public String getTableName() {
            return tableName;
        }

        public List<Predicate> getPredicates() {
            return predicates;
        }

        public WriteMode getWriteMode() {
            return writeMode;
        }

        public static KuduOutputFormat.Conf.ConfBuilder builder() {
            return new KuduOutputFormat.Conf.ConfBuilder();
        }

        public static class ConfBuilder implements Serializable {

            private KuduOutputFormat.Conf conf;

            public ConfBuilder() {
                this.conf = new Conf();
            }

            public ConfBuilder masterAddress(String masterAddress) {
                this.conf.masterAddress = masterAddress;
                return this;
            }

            public ConfBuilder tableName(String tableName) {
                this.conf.tableName = tableName;
                return this;
            }

            public ConfBuilder addPredicate(Predicate predicate) {
                this.conf.predicates.add(predicate);
                return this;
            }

            public ConfBuilder writeMode(WriteMode mode) {
                this.conf.writeMode = mode;
                return this;
            }

            public Conf build() {
                return this.conf;
            }
        }
    }
}