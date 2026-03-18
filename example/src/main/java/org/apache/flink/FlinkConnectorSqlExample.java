/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink;

import com.vesoft.nebula.driver.graph.data.ResultSet;
import com.vesoft.nebula.driver.graph.net.NebulaClient;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink SQL Example for NebulaGraph connector.
 * Demonstrates how to use Flink SQL to read and write NebulaGraph.
 */
public class FlinkConnectorSqlExample {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkConnectorSqlExample.class);

    public static void main(String[] args) throws Exception {
        // Prepare NebulaGraph schema and data
        prepareGraph();

        // Create Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Example 1: Create a table for reading NebulaGraph node data
        String createNodeTableSql =
                "CREATE TABLE nebula_node_source (\n"
                        + "  col1 STRING,\n"
                        + "  col2 STRING,\n"
                        + "  col3 TINYINT,\n"
                        + "  col4 SMALLINT,\n"
                        + "  col5 INT,\n"
                        + "  col6 BIGINT,\n"
                        + "  col7 DATE,\n"
                        + "  col8 TIMESTAMP(3),\n"
                        + "  col9 TIMESTAMP WITH LOCAL TIME ZONE,\n"
                        + "  col10 BOOLEAN,\n"
                        + "  col11 DOUBLE,\n"
                        + "  col12 FLOAT,\n"
                        + "  col13 TIMESTAMP WITH LOCAL TIME ZONE\n"
                        + ") WITH (\n"
                        + "  'connector' = 'nebula',\n"
                        + "  'graph-name' = 'flinkSqlGraph',\n"
                        + "  'label-name' = 'person',\n"
                        + "  'data-type' = 'NODE',\n"
                        + "  'graph-address' = '192.168.8.6:3820',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = 'NebulaGraph01',\n"
                        + "  'batch-size' = '10'\n"
                        + ")";
        tableEnv.executeSql(createNodeTableSql);
        LOG.info("Created node source table successfully");

        // Example 2: Query node data from NebulaGraph
        TableResult nodeQueryResult = tableEnv.executeSql("SELECT * FROM nebula_node_source");
        LOG.info("Query node data from NebulaGraph:");
        nodeQueryResult.print();

        // Example 3: Create a table for reading NebulaGraph edge data
        String createEdgeTableSql =
                "CREATE TABLE nebula_edge_source (\n"
                        + "  src_col STRING,\n"
                        + "  dst_col STRING,\n"
                        + "  col1 STRING,\n"
                        + "  col2 STRING,\n"
                        + "  col3 TINYINT,\n"
                        + "  col4 SMALLINT,\n"
                        + "  col5 INT,\n"
                        + "  col6 BIGINT,\n"
                        + "  col7 DATE,\n"
                        + "  col8 TIMESTAMP(3),\n"
                        + "  col9 TIMESTAMP WITH LOCAL TIME ZONE,\n"
                        + "  col10 BOOLEAN,\n"
                        + "  col11 DOUBLE,\n"
                        + "  col12 FLOAT,\n"
                        + "  col13 TIMESTAMP WITH LOCAL TIME ZONE\n"
                        + ") WITH (\n"
                        + "  'connector' = 'nebula',\n"
                        + "  'graph-name' = 'flinkSqlGraph',\n"
                        + "  'label-name' = 'friend',\n"
                        + "  'data-type' = 'EDGE',\n"
                        + "  'graph-address' = '192.168.8.6:3820',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = 'NebulaGraph01',\n"
                        + "  'batch-size' = '10',\n"
                        + "  'src-pk-columns' = 'src_col',\n"
                        + "  'dst-pk-columns' = 'dst_col'\n"
                        + ")";
        tableEnv.executeSql(createEdgeTableSql);
        LOG.info("Created edge source table successfully");

        // Example 4: Query edge data from NebulaGraph
        TableResult edgeQueryResult = tableEnv.executeSql("SELECT * FROM nebula_edge_source");
        LOG.info("Query edge data from NebulaGraph:");
        edgeQueryResult.print();

        // Example 5: Create a table for writing node data to NebulaGraph
        String createNodeSinkTableSql =
                "CREATE TABLE nebula_node_sink (\n"
                        + "  col1 STRING,\n"
                        + "  col2 STRING,\n"
                        + "  col3 TINYINT,\n"
                        + "  col4 SMALLINT,\n"
                        + "  col5 INT,\n"
                        + "  col6 BIGINT,\n"
                        + "  col7 DATE,\n"
                        + "  col8 TIMESTAMP(3),\n"
                        + "  col9 TIME,\n"
                        + "  col10 BOOLEAN,\n"
                        + "  col11 DOUBLE,\n"
                        + "  col12 FLOAT,\n"
                        + "  col13 TIMESTAMP WITH LOCAL TIME ZONE,\n"
                        + "  PRIMARY KEY (col1) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'nebula',\n"
                        + "  'graph-name' = 'flinkSqlGraph',\n"
                        + "  'label-name' = 'person',\n"
                        + "  'data-type' = 'NODE',\n"
                        + "  'graph-address' = '192.168.8.6:3820',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = 'NebulaGraph01',\n"
                        + "  'write-mode' = 'INSERTREPLACE',\n"
                        + "  'batch-size' = '10',\n"
                        + "  'pk-columns' = 'col1'\n"
                        + ")";
        tableEnv.executeSql(createNodeSinkTableSql);
        LOG.info("Created node sink table successfully");

        // Example 6: Insert node data into NebulaGraph using SQL
        tableEnv.executeSql(
                "INSERT INTO nebula_node_sink VALUES "
                        + "('100', 'Alice', CAST(1 AS TINYINT), CAST(1111 AS SMALLINT), 22222,"
                        + " 6412233, DATE '2019-01-01', TIMESTAMP '2019-01-01 12:12:12', TIME "
                        + "'10:10:10', false, 1.2, 1.0, TIMESTAMP WITH LOCAL TIME ZONE '11:12:12 Z'"
                        + "),"
                        + "('101', 'Bob', CAST(1 AS TINYINT), CAST(1111 AS SMALLINT), 22222, "
                        + "6412233, DATE '2019-01-01', "
                        + "TIMESTAMP '2019-01-01 12:12:12', TIME '10:10:10', true, "
                        + "1.2, 1.0, TIMESTAMP WITH LOCAL TIME ZONE '11:12:12 Z')");
        LOG.info("Inserted node data into NebulaGraph successfully");

        // Example 7: Create a table for writing edge data to NebulaGraph
        String createEdgeSinkTableSql =
                "CREATE TABLE nebula_edge_sink (\n"
                        + "  src_col1 STRING,\n"
                        + "  dst_col1 STRING,\n"
                        + "  col1 STRING,\n"
                        + "  col2 STRING,\n"
                        + "  col3 TINYINT,\n"
                        + "  col4 SMALLINT,\n"
                        + "  col5 INT,\n"
                        + "  col6 BIGINT,\n"
                        + "  col7 DATE,\n"
                        + "  col8 TIMESTAMP(3),\n"
                        + "  col9 TIME,\n"
                        + "  col10 BOOLEAN,\n"
                        + "  col11 DOUBLE,\n"
                        + "  col12 FLOAT,\n"
                        + "  col13 TIMESTAMP WITH LOCAL TIME ZONE,\n"
                        + "  PRIMARY KEY (src_col1, dst_col1) NOT ENFORCED\n"
                        + ") WITH (\n"
                        + "  'connector' = 'nebula',\n"
                        + "  'graph-name' = 'flinkSqlGraph',\n"
                        + "  'label-name' = 'friend',\n"
                        + "  'data-type' = 'EDGE',\n"
                        + "  'graph-address' = '192.168.8.6:3820',\n"
                        + "  'username' = 'root',\n"
                        + "  'password' = 'NebulaGraph01',\n"
                        + "  'write-mode' = 'INSERTREPLACE',\n"
                        + "  'batch-size' = '10',\n"
                        + "  'src-pk-columns' = 'src_col1',\n"
                        + "  'dst-pk-columns' = 'dst_col1',\n"
                        + "  'edge-src-pks' = 'col1',\n"
                        + "  'edge-dst-pks' = 'col1'\n"
                        + ")";
        tableEnv.executeSql(createEdgeSinkTableSql);
        LOG.info("Created edge sink table successfully");

        // Example 8: Insert edge data into NebulaGraph using SQL
        tableEnv.executeSql("INSERT INTO nebula_edge_sink VALUES "
                                    + "('100', '101', 'friendship', 'close', CAST(1 AS TINYINT), "
                                    + "CAST(1111 AS SMALLINT), 22222, "
                                    + "6412233, DATE '2019-01-01', TIMESTAMP '2019-01-01 12:12:12',"
                                    + " TIME '15:10:00', false, 1.2, 1.0, TIMESTAMP WITH LOCAL "
                                    + "TIME ZONE '11:12:12 Z')");
        LOG.info("Inserted edge data into NebulaGraph successfully");

        // Example 9: Join node data and insert into another graph
        Table joinedData = tableEnv.sqlQuery(
                "SELECT src.col1 as src_col1, dst.col1 as dst_col1, 'new_friend' as col1, "
                        + "'new_relation' as col2, CAST(1 AS TINYINT) as col3, CAST(1111 AS "
                        + "SMALLINT) as col4, 22222 as col5, "
                        + "6412233 as col6, DATE '2019-01-01' as col7, "
                        + "TIMESTAMP '2019-01-01 12:12:12' as col8, TIME '15:10:00' as col9, "
                        + "false as col10, 1.2 as col11, 1.0 as col12, TIME '11:12:12 Z' "
                        + "as col13 FROM nebula_node_source src, nebula_node_source dst "
                        + "WHERE src.col1 = '100' AND dst.col1 = '101'");
        joinedData.executeInsert("nebula_edge_sink");
        LOG.info("Inserted joined data into NebulaGraph successfully");

        LOG.info("Flink SQL Example completed successfully!");
    }

    /**
     * Prepare NebulaGraph schema and sample data
     */
    private static void prepareGraph() {
        String graphType = "CREATE GRAPH TYPE IF NOT EXISTS flinkSqlType AS{\n"
                + "NODE TYPE person(LABEL person{col1 string primary key, col2 string, col3 int8,"
                + " col4 int16,col5 int32, col6 int64, col7 date, col8 local datetime, "
                + "col9 local time, col10 bool, col11 double, col12 float, col13 zoned time}),\n"
                + "EDGE TYPE friend(person)-[LABEL friend{col1 string, col2 string, col3 int8, "
                + "col4 int16, col5 int32, col6 int64, col7 date, col8 local datetime, "
                + "col9 local time, col10 bool, col11 double,col12 float, col13 zoned time}]"
                + "->(person)\n"
                + " }";
        String       graph  = "CREATE GRAPH IF NOT EXISTS flinkSqlGraph TYPED flinkSqlType";
        NebulaClient client = null;
        try {
            client = NebulaClient
                    .builder("192.168.8.6:3820", "root", "NebulaGraph01")
                    .build();
            ResultSet res = client.execute(graphType);
            if (!res.isSucceeded()) {
                LOG.error("create graph type failed:" + res.getErrorMessage());
                System.exit(1);
            }
            res = client.execute(graph);
            if (!res.isSucceeded()) {
                LOG.error("create graph failed:" + res.getErrorMessage());
                System.exit(1);
            }

            client.execute("SESSION SET zoned_time_format=\"%H:%M:%S\"");

            // Insert sample node data
            String insertNode;
            for (int i = 0; i < 10; i++) {
                insertNode = String.format(
                        "TABLE t{c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12} = \n"
                                + "(\"%d\",\"name_%d\",1,1111,22222,6412233,date(\"2019-01-01\"),"
                                + "local_datetime(\"2019-01-01T12:12:12\"),local_time(\"10:10:10\")"
                                + ",false,1.2,1.0,zoned_time(\"11:12:12\")) \n"
                                + "USE `flinkSqlGraph` \n"
                                + "FOR r IN t \n"
                                + "INSERT OR REPLACE (@`person`{`col1`:r.c0,`col2`:r.c1,`col3`:"
                                + "cast(r.c2 as INT8),`col4`:cast(r.c3 as INT16),`col5`:r.c4,"
                                + "`col6`:r.c5,`col7`:r.c6,`col8`:r.c7,`col9`:r.c8,`col10`:r.c9,"
                                + "`col11`:r.c10,`col12`:r.c11,`col13`:r.c12})", i, i);
                res = client.execute(insertNode);
                if (!res.isSucceeded()) {
                    LOG.error("insert node failed:" + res.getErrorMessage());
                    System.exit(1);
                }
            }

            // Insert sample edge data
            String insertEdge;
            for (int i = 0; i < 10; i++) {
                for (int j = 0; j < 3; j++) {
                    insertEdge = String.format(
                            "TABLE t{src_0,dst_0,c0,c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12} = \n"
                                    + "(\"%d\",\"%d\",\"edge_%d\",\"desc_%d\",1,1111,22222,6412233,"
                                    + "date(\"2019-01-01\"),local_datetime(\"2019-01-01T12:12:12\")"
                                    + ",local_time(\"15:10:00\"),false,1.2,1.0,"
                                    + "zoned_time(\"11:12:12\")) \n"
                                    + "USE `flinkSqlGraph` \n"
                                    + "FOR r IN t \n"
                                    + "OPTIONAL MATCH (n_src@`person`) WHERE n_src.`col1`=r.src_0 "
                                    + "OPTIONAL MATCH (n_dst@`person`) WHERE n_dst.`col1`=r.dst_0\n"
                                    + "INSERT OR IGNORE (n_src)-[@`friend`{`col1`:r.c0,`col2`:r.c1,"
                                    + "`col3`:cast(r.c2 as INT8),`col4`:cast(r.c3 as INT16),"
                                    + "`col5`:r.c4,`col6`:r.c5,`col7`:r.c6,`col8`:r.c7,`col9`:r.c8,"
                                    + "`col10`:r.c9,`col11`:r.c10,`col12`:r.c11,`col13`:r.c12}]"
                                    + "->(n_dst)", i, j, i, j);
                    res = client.execute(insertEdge);
                    if (!res.isSucceeded()) {
                        LOG.error("insert edge failed:" + res.getErrorMessage());
                        System.exit(1);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (client != null) {
                client.close();
            }
        }
        LOG.info("prepare graph finished!");
    }
}
