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

package kudu.stream;

import kudu.batch.KuduOutputFormat;
import kudu.internal.KuduUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.Preconditions;

import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.OperationResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static kudu.internal.KuduUtils.insertTuple;


/**
 * This class is in charge of inserting Flink tuples in Kudu.
 *
 * @param <IN> Class extending from org.apache.flink.api.java.tuple.Tuple
 */
public class KuduSink<IN extends Tuple> extends RichSinkFunction<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(KuduOutputFormat.class);

    private KuduOutputFormat.Conf conf;

    private transient KuduTable table;

    private transient KuduSession session;

    private transient KuduClient client;

    public KuduSink(KuduOutputFormat.Conf conf) {
        this.conf = conf;
    }

    @Override
    public void open(Configuration parameters) {
        // Check hat client has been properly initialized
        this.client = Preconditions.checkNotNull(KuduUtils.newClient(conf.getMasterAddress()),
                "Invalid Kudu master address");

        // Check that table handler is not null and table exists
        this.table = Preconditions.checkNotNull(KuduUtils.getTable(conf.getTableName(), this.client),
                "Kudu table cannot be null. Check if your table actually exists");

        this.session = Preconditions.checkNotNull(KuduUtils.createSession(this.client),
                "Kudu session cannot be null");

        // Check that te number of columns in table is at least one
        Preconditions.checkArgument(this.table.getSchema().getColumnCount() > 0,
                "The total number of columns in your Kudu table should be at least 1");
    }


    @Override
    public void invoke(IN tuple) throws Exception {
        OperationResponse op = insertTuple(tuple, this.table, this.session, conf.getWriteMode());
        if(op.hasRowError()){
            LOG.warn("An error occurred trying to insert a record in Kudu : {} ", op.getRowError());
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(session != null){
            session.close();
        }
        if(client != null){
            client.shutdown();
        }
    }
}