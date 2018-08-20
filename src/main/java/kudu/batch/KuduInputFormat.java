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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import kudu.internal.Predicate;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.util.Preconditions;

import org.apache.kudu.client.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static kudu.internal.KuduUtils.rowResultToTuple;
import static kudu.internal.Predicate.toKuduPredicates;


public class KuduInputFormat<OUT extends Tuple> extends RichInputFormat<OUT, KuduInputSplit> {

    private transient KuduClient client;

    private transient KuduTable table;

    private transient List<KuduPredicate> predicates;

    private transient KuduScanner scanner;

    private transient RowResultIterator resultIterator;

    private boolean endReached = false;

    private Conf conf;

    private static final Logger LOG = LoggerFactory.getLogger(KuduInputFormat.class);

    public KuduInputFormat(Conf conf) {
        this.conf = Preconditions.checkNotNull(conf, "Kudu config cannot be null");
    }

    @Override
    public void configure(Configuration parameters) {
        this.client = new KuduClient.KuduClientBuilder(conf.masterAddress)
                .build();
        try {
            this.table = this.client.openTable(conf.tableName);
        } catch (KuduException e) {
            e.printStackTrace();
        }
        this.predicates = toKuduPredicates(conf.predicates, this.table.getSchema());
    }


    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public KuduInputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (client == null) {
            throw new IOException("Kudu client cannot be null");
        }

        if (table == null) {
            throw new IOException("Kudu table cannot be null");
        }

        KuduScanToken.KuduScanTokenBuilder tokenBuilder = getKuduScanTokenBuilder();

        List<KuduScanToken> tokens = tokenBuilder.build();

        KuduInputSplit[] splits = new KuduInputSplit[tokens.size()];

        for (int i = 0; i < tokens.size(); i++) {
            KuduScanToken token = tokens.get(i);

            List<String> locations = new ArrayList<>(token.getTablet().getReplicas().size());

            for (LocatedTablet.Replica replica : token.getTablet().getReplicas()) {
                locations.add(getLocation(replica.getRpcHost(), replica.getRpcPort()));
            }

            KuduInputSplit split = new KuduInputSplit(
                    token.serialize(),
                    i,
                    locations.toArray(new String[locations.size()])
            );
            splits[i] = split;
        }

        if (splits.length < minNumSplits) {
            LOG.warn(" The minimum desired number of splits with your configured parallelism level " +
                            "is {}. Current kudu splits = {}. {} instances will remain idle.",
                    minNumSplits,
                    splits.length,
                    (minNumSplits - splits.length)
            );
        }

        return splits;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(KuduInputSplit[] inputSplits) {
        return new LocatableInputSplitAssigner(inputSplits);
    }

    @Override
    public void open(KuduInputSplit split) throws IOException {
        endReached = false;

        scanner = KuduScanToken.deserializeIntoScanner(split.getScanToken(), client);

        resultIterator = scanner.nextRows();
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return endReached;
    }

    @Override
    public OUT nextRecord(OUT reuse) throws IOException {
        // check that current iterator has next rows
        if (this.resultIterator.hasNext()) {
            RowResult row = this.resultIterator.next();
            return rowResultToTuple(row);
        }
        // if not, check that current scanner has more iterators
        else if (scanner.hasMoreRows()) {
            this.resultIterator = scanner.nextRows();
            return nextRecord(reuse);
        }
        else {
            endReached = true;
        }
        return null;
    }

    @Override
    public void close() {
        if (scanner != null) {
            try {
                scanner.close();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * Returns a endpoint url in the following format: <host>:<ip>
     *
     * @param host Hostname
     * @param port Port
     * @return Formatted URL
     */
    private String getLocation(String host, Integer port) {
        StringBuilder builder = new StringBuilder();
        builder.append(host).append(":").append(port);
        return builder.toString();
    }

    private KuduScanToken.KuduScanTokenBuilder getKuduScanTokenBuilder() {
        KuduScanToken.KuduScanTokenBuilder tokenBuilder = client
                .newScanTokenBuilder(table);

        if (conf.projection != null && conf.projection.size() > 0) {
            tokenBuilder.setProjectedColumnNames(conf.projection);
        }

        for (KuduPredicate predicate : predicates) {
            tokenBuilder.addPredicate(predicate);
        }

        if (conf.limit > 0) {
            tokenBuilder.limit(conf.limit); // FIXME: https://issues.apache.org/jira/browse/KUDU-16
            // Server side limit() operator for java-based scanners are not implemented yet

        }

        return tokenBuilder;
    }

    /**
     * A serializable, configuration class that allows to configure a KuduInputFormat.
     * This class provides a fluent API to ease users to provide configuration in a easy way.
     */
    public static class Conf implements Serializable {
        private String masterAddress;

        private String tableName;

        private List<Predicate> predicates = new ArrayList<>();

        private List<String> projection = new ArrayList<>();

        private int limit;

        public String getMasterAddress() {
            return masterAddress;
        }

        public String getTableName() {
            return tableName;
        }

        public List<Predicate> getPredicates() {
            return predicates;
        }

        public List<String> getProjection() {
            return projection;
        }

        public int getLimit() {
            return limit;
        }

        private Conf() {
        }

        public static ConfBuilder builder() {
            return new ConfBuilder();
        }

        public static class ConfBuilder implements Serializable {
            private Conf conf;

            private static final Logger LOG = LoggerFactory.getLogger(ConfBuilder.class);


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

            public ConfBuilder project(String... fields) {
                this.conf.projection = Arrays.asList(fields);
                return this;
            }

            public ConfBuilder limit(int numOfRecords) {
                LOG.warn("Kudu Limit will be ignored since this feature is not yet implemented" +
                        "in the scanner API. See: https://issues.apache.org/jira/browse/KUDU-16");
                if (numOfRecords <= 0) {
                    throw new IllegalArgumentException("Limit cannot be <= 0");
                }

                this.conf.limit = numOfRecords;
                return this;
            }

            public Conf build() {
                return this.conf;
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Conf conf = (Conf) o;

            if (masterAddress != null ? !masterAddress.equals(conf.masterAddress) : conf.masterAddress != null) {
                return false;
            }
            if (tableName != null ? !tableName.equals(conf.tableName) : conf.tableName != null) {
                return false;
            }
            if (predicates != null ? !predicates.equals(conf.predicates) : conf.predicates != null) {
                return false;
            }
            return projection != null ? projection.equals(conf.projection) : conf.projection == null;
        }

        @Override
        public int hashCode() {
            int result = masterAddress != null ? masterAddress.hashCode() : 0;
            result = 31 * result + (tableName != null ? tableName.hashCode() : 0);
            result = 31 * result + (predicates != null ? predicates.hashCode() : 0);
            result = 31 * result + (projection != null ? projection.hashCode() : 0);
            return result;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        KuduInputFormat<?> that = (KuduInputFormat<?>) o;

        if (endReached != that.endReached) {
            return false;
        }
        if (client != null ? !client.equals(that.client) : that.client != null) {
            return false;
        }
        if (table != null ? !table.equals(that.table) : that.table != null) {
            return false;
        }
        if (predicates != null ? !predicates.equals(that.predicates) : that.predicates != null) {
            return false;
        }
        if (scanner != null ? !scanner.equals(that.scanner) : that.scanner != null) {
            return false;
        }
        if (conf != null ? !conf.equals(that.conf) : that.conf != null) {
            return false;
        }
        return resultIterator != null ? resultIterator.equals(that.resultIterator) : that.resultIterator == null;
    }

    @Override
    public int hashCode() {
        int result = client != null ? client.hashCode() : 0;
        result = 31 * result + (table != null ? table.hashCode() : 0);
        result = 31 * result + (predicates != null ? predicates.hashCode() : 0);
        result = 31 * result + (scanner != null ? scanner.hashCode() : 0);
        result = 31 * result + (endReached ? 1 : 0);
        result = 31 * result + (conf != null ? conf.hashCode() : 0);
        result = 31 * result + (resultIterator != null ? resultIterator.hashCode() : 0);
        return result;
    }


    @Override
    public void closeInputFormat() throws IOException {
        if (client != null) {
            try {
                client.shutdown();
            } catch (KuduException e) {
                e.printStackTrace();
            }
        }
    }
}