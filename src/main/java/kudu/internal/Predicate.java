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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;

import static kudu.internal.Predicate.ComparisonOp.*;

/**
 * This class wraps the KuduPredicate class which is noy serializable and is needed to create Kudu queries.
 * It also provides a fluent API to easily create Kudu predicates.
 */
public class Predicate implements Serializable {

    /**
     * Column key can be the column name or the column index
     */
    private Object columnKey;

    /**
     * Boolean that shows whether the columnName is a name or an index
     */
    private boolean isColumnByName;

    /**
     * Predicate value
     */
    private Object val;

    /**
     * A comparison operator
     */
    private ComparisonOp comparisonOperator;


    private Predicate() {
    }

    public static PredicateBuilder columnByIndex(int idx) {
        return new PredicateBuilder(idx);
    }

    public static PredicateBuilder columnByName(String name) {
        return new PredicateBuilder(name);
    }

    public static class PredicateBuilder {

        private Predicate predicate;

        public PredicateBuilder() {
            this.predicate = new Predicate();
        }

        public PredicateBuilder(int columnIndex) {
            this.predicate = new Predicate();
            this.predicate.columnKey = columnIndex;
            this.predicate.isColumnByName = false;
        }

        public PredicateBuilder(String columnName) {
            this();
            this.predicate.columnKey = columnName;
            this.predicate.isColumnByName = true;
        }

        public PredicateBuilder isEqualTo() {
            this.predicate.comparisonOperator = EQUAL;
            return this;
        }

        public PredicateBuilder isGreaterThan() {
            this.predicate.comparisonOperator = GREATER;
            return this;
        }

        public PredicateBuilder isGreaterOrEqual() {
            this.predicate.comparisonOperator = GREATER_EQUAL;
            return this;
        }

        public PredicateBuilder isLessThan() {
            this.predicate.comparisonOperator = LESS;
            return this;
        }

        public PredicateBuilder isLessOrEqualThan() {
            this.predicate.comparisonOperator = LESS_EQUAL;
            return this;
        }

        public Predicate val(Object val) {
            this.predicate.val = val;
            // return this;
            return this.predicate;
        }
    }

    public enum ComparisonOp {
        GREATER,
        GREATER_EQUAL,
        EQUAL,
        LESS,
        LESS_EQUAL,
    }

    /**
     * Converts a comparison operator into a kudu comparison operator
     *
     * @param comparisonOp a comparison operator
     * @return a kudu comparison operator
     */
    private static KuduPredicate.ComparisonOp toKuduComparisonOp(ComparisonOp comparisonOp) {
        switch (comparisonOp) {
            case LESS:
                return KuduPredicate.ComparisonOp.LESS;
            case LESS_EQUAL:
                return KuduPredicate.ComparisonOp.LESS_EQUAL;
            case GREATER:
                return KuduPredicate.ComparisonOp.GREATER;
            case GREATER_EQUAL:
                return KuduPredicate.ComparisonOp.GREATER_EQUAL;
            case EQUAL:
                return KuduPredicate.ComparisonOp.EQUAL;
            default:
                throw new IllegalArgumentException("Illegal comparison operator: " + comparisonOp);
        }
    }

    /**
     * Converts a predicate into a kudu predicate
     *
     * @param predicate predicate
     * @param schema    Table schema
     * @return a kudu predicate
     */
    private static KuduPredicate toKuduPredicate(Predicate predicate, Schema schema) {
        ColumnSchema columnSchema = predicate.isColumnByName
                ? schema.getColumn((String) predicate.columnKey)
                : schema.getColumnByIndex((Integer) predicate.columnKey);
        String clazz = predicate.val.getClass().getSimpleName();

        switch (clazz) {
            case "String":
                return KuduPredicate.newComparisonPredicate(
                        columnSchema,
                        toKuduComparisonOp(predicate.comparisonOperator),
                        (String) predicate.val
                );
            case "Integer":
                return KuduPredicate.newComparisonPredicate(
                        columnSchema,
                        toKuduComparisonOp(predicate.comparisonOperator),
                        (Integer) predicate.val
                );
            default:
                throw new IllegalArgumentException(clazz + " is not a valid data type");
        }
    }

    /**
     * Converts a set of predicate into kudu predicates by using toKuduPredicate method
     *
     * @param predicates set of predicates
     * @param schema     table schema
     * @return a set of kudu predicates
     */
    public static List<KuduPredicate> toKuduPredicates(List<Predicate> predicates, Schema schema) {
        List<KuduPredicate> kuduPredicates = new ArrayList<>();
        if (predicates != null) {
            for (Predicate predicate : predicates) {
                kuduPredicates.add(toKuduPredicate(predicate, schema));
            }
        }
        return kuduPredicates;
    }

    @Override
    public String toString() {
        return "Predicate{" +
                "columnKey=" + columnKey +
                ", val=" + val +
                ", comparisonOperator=" + comparisonOperator +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Predicate predicate = (Predicate) o;

        if (isColumnByName != predicate.isColumnByName) {
            return false;
        }
        if (columnKey != null ? !columnKey.equals(predicate.columnKey) : predicate.columnKey != null) {
            return false;
        }
        if (val != null ? !val.equals(predicate.val) : predicate.val != null) {
            return false;
        }
        return comparisonOperator == predicate.comparisonOperator;
    }

    @Override
    public int hashCode() {
        int result = columnKey != null ? columnKey.hashCode() : 0;
        result = 31 * result + (isColumnByName ? 1 : 0);
        result = 31 * result + (val != null ? val.hashCode() : 0);
        result = 31 * result + (comparisonOperator != null ? comparisonOperator.hashCode() : 0);
        return result;
    }
}