/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula;

public class MockData {

    public static String createFlinkSinkGraphType() {
        return "CREATE GRAPH TYPE IF NOT EXISTS flink_sink_type AS {"
                + "NODE TYPE person(LABEL person{col1 STRING PRIMARY KEY,col2 STRING,"
                + "col3 INT32,col4 DOUBLE,col5 DATE,col6 ZONED DATETIME,"
                + "col7 ZONED TIME,col8 INT64}),"
                + "EDGE TYPE friend(person)-[LABEL friend{col1 STRING,col2 STRING,col3 INT32,"
                + "col4 DOUBLE,col5 DATE,col6 ZONED DATETIME,col7 ZONED TIME,col8 INT64}]->(person)"
                + "}";
    }

    public static String createFlinkSinkGraph() {
        return "CREATE GRAPH IF NOT EXISTS flink_sink_test TYPED flink_sink_type";

    }


    public static String createFlinkSourceGraphType() {
        return "CREATE GRAPH TYPE IF NOT EXISTS flink_source_type AS {"
                + "NODE TYPE person(LABEL person{col1 INT64 PRIMARY KEY,col2 STRING,"
                + "col3 STRING,col4 INT8,col5 INT16,col6 INT32,col7 INT64,col8 DATE,"
                + "col9 LOCAL DATETIME,col10 LOCAL TIME,col11 ZONED DATETIME,col12 ZONED TIME, "
                + "col13 BOOL}),"
                + "EDGE TYPE friend(person)-[LABEL friend{col1 INT64,col2 STRING,col3 STRING,"
                + "col4 INT8,col5 INT16,col6 INT32,col7 INT64,col8 DATE,col9 LOCAL DATETIME,"
                + "col10 LOCAL TIME,col11 ZONED DATETIME,col12 ZONED TIME, col13 BOOL}]->(person)"
                + "}";
    }

    public static String createFlinkSourceGraph() {
        return "CREATE GRAPH IF NOT EXISTS flink_source_test TYPED flink_source_type";

    }

}
