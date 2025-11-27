/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

//package org.apache.flink.connector.nebula.sink;
//
//import static org.apache.flink.connector.nebula.NebulaValueUtils.dateOf;
//import static org.apache.flink.connector.nebula.NebulaValueUtils.dateTimeOf;
//import static org.apache.flink.connector.nebula.NebulaValueUtils.pointOf;
//import static org.apache.flink.connector.nebula.NebulaValueUtils.rowOf;
//import static org.apache.flink.connector.nebula.NebulaValueUtils.timeOf;
//import static org.apache.flink.connector.nebula.NebulaValueUtils.valueOf;
//
//import java.util.Arrays;
//import java.util.concurrent.ExecutionException;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.nebula.MockData;
//import org.apache.flink.connector.nebula.NebulaITTestBase;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.StatementSet;
//import org.apache.flink.table.api.TableEnvironment;
//import org.junit.AfterClass;
//import org.junit.Before;
//import org.junit.BeforeClass;
//import org.junit.Test;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class AbstractNebulaOutputFormatITTest extends NebulaITTestBase {
//
//    private static final Logger LOGGER =
//            LoggerFactory.getLogger(AbstractNebulaOutputFormatITTest.class);
//
//    private static TableEnvironment tableEnvironment;
//
//    @BeforeClass
//    public static void beforeAll() {
//        initializeNebulaClient();
//        initializeNebulaSchema(MockData.createFlinkTestSpace());
//    }
//
//    @AfterClass
//    public static void afterAll() {
//        closeGraphProvider();
//    }
//
//    @Before
//    public void before() {
//        tableEnvironment = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
//    }
//
//    /**
//     * sink Nebula Graph Vertex Data with default INSERT mode
//     */
//    @Test
//    public void testSinkVertexData() throws ExecutionException, InterruptedException {
//        Configuration configuration = tableEnvironment.getConfig().getConfiguration();
//        configuration.setString("table.dml-sync", "true");
//
//        tableEnvironment.executeSql(
//                "CREATE TABLE person ("
//                        + "vid STRING,"
//                        + "col1 STRING,"
//                        + "col2 STRING,"
//                        + "col3 STRING,"
//                        + "col4 STRING,"
//                        + "col5 STRING,"
//                        + "col6 STRING,"
//                        + "col7 STRING,"
//                        + "col8 STRING,"
//                        + "col9 STRING,"
//                        + "col10 STRING,"
//                        + "col11 STRING,"
//                        + "col12 STRING,"
//                        + "col13 STRING,"
//                        + "col14 STRING"
//                        + ")"
//                        + "WITH ("
//                        + "'connector'='nebula',"
//                        + "'graph-address'='"
//                        + GRAPH_ADDRESS
//                        + "',"
//                        + "'username'='"
//                        + USERNAME
//                        + "',"
//                        + "'password'='"
//                        + PASSWORD
//                        + "',"
//                        + "'graph-graph'='flink_test',"
//                        + "'label-name'='person',"
//                        + "'data-type'='node'"
//                        + ")"
//                                   );
//
//        StatementSet stmtSet = tableEnvironment.createStatementSet();
//        stmtSet.addInsertSql(
//                "INSERT INTO person"
//                        + " VALUES ('61', 'aba', 'abcdefgh', '1', '1111',"
//                        + " '22222', '6412233', '2019-01-01', '2019-01-01T12:12:12',"
//                        + " '435463424', 'false', '1.2', '1.0', '11:12:12', 'POINT(1 3)')"
//                            );
//        stmtSet.addInsertSql(
//                "INSERT INTO person"
//                        + " VALUES ('62', 'aba', 'abcdefgh', '1', '1111',"
//                        + " '22222', '6412233', '2019-01-01', '2019-01-01T12:12:12',"
//                        + " '435463424', 'false', '1.2', '1.0', '11:12:12', 'POINT(1 3)')"
//                            );
//        stmtSet.addInsertSql(
//                "INSERT INTO person"
//                        + " VALUES ('89', 'aba', 'abcdefgh', '1', '1111',"
//                        + " '22222', '6412233', '2019-01-01', '2019-01-01T12:12:12',"
//                        + " '435463424', 'false', '1.2', '1.0', '11:12:12', 'POINT(1 3)')"
//                            );
//        stmtSet.execute().await();
//
//        check(
//                Arrays.asList(
//                        rowOf(
//                                valueOf(61), valueOf("aba"), valueOf("abcdefgh"),
//                                valueOf(1), valueOf(1111), valueOf(22222), valueOf(6412233),
//                                dateOf(2019, 1, 1), dateTimeOf(2019, 1, 1, 4, 12, 12, 0),
//                                valueOf(435463424), valueOf(false), valueOf(1.2), valueOf(1.0),
//                                timeOf(3, 12, 12, 0), pointOf(1.0, 3.0)
//                             ),
//                        rowOf(
//                                valueOf(62), valueOf("aba"), valueOf("abcdefgh"),
//                                valueOf(1), valueOf(1111), valueOf(22222), valueOf(6412233),
//                                dateOf(2019, 1, 1), dateTimeOf(2019, 1, 1, 4, 12, 12, 0),
//                                valueOf(435463424), valueOf(false), valueOf(1.2), valueOf(1.0),
//                                timeOf(3, 12, 12, 0), pointOf(1.0, 3.0)
//                             ),
//                        rowOf(
//                                valueOf(89), valueOf("aba"), valueOf("abcdefgh"),
//                                valueOf(1), valueOf(1111), valueOf(22222), valueOf(6412233),
//                                dateOf(2019, 1, 1), dateTimeOf(2019, 1, 1, 4, 12, 12, 0),
//                                valueOf(435463424), valueOf(false), valueOf(1.2), valueOf(1.0),
//                                timeOf(3, 12, 12, 0), pointOf(1.0, 3.0)
//                             )
//                             ),
//                "MATCH (n)"
//                        + " WHERE id(n) IN [61, 62, 89] RETURN id(n) AS key,"
//                        + " n.person.col1, n.person.col2, n.person.col3, n.person.col4,"
//                        + " n.person.col5, n.person.col6, n.person.col7, n.person.col8,"
//                        + " n.person.col9, n.person.col10, n.person.col11, n.person.col12,"
//                        + " n.person.col13, n.person.col14"
//                        + " ORDER BY key LIMIT 10"
//             );
//
//    }
//
//
//    /**
//     * sink Nebula Graph Edge Data with default INSERT mode
//     */
//    @Test
//    public void testSinkEdgeData() throws ExecutionException, InterruptedException {
//        check(null, "INSERT vertex person() VALUES 61:()");
//        check(null, "INSERT vertex person() VALUES 62:()");
//
//        tableEnvironment.executeSql(
//                "CREATE TABLE friend ("
//                        + "src STRING,"
//                        + "dst STRING,"
//                        + "col1 STRING,"
//                        + "col2 STRING,"
//                        + "col3 STRING,"
//                        + "col4 STRING,"
//                        + "col5 STRING,"
//                        + "col6 STRING,"
//                        + "col7 STRING,"
//                        + "col8 STRING,"
//                        + "col9 STRING,"
//                        + "col10 STRING,"
//                        + "col11 STRING,"
//                        + "col12 STRING,"
//                        + "col13 STRING,"
//                        + "col14 STRING"
//                        + ")"
//                        + "WITH ("
//                        + "'connector'='nebula',"
//                        + "'graph-address'='"
//                        + GRAPH_ADDRESS
//                        + "',"
//                        + "'username'='"
//                        + USERNAME
//                        + "',"
//                        + "'password'='"
//                        + PASSWORD
//                        + "',"
//                        + "'graph-space'='flink_test',"
//                        + "'label-name'='friend',"
//                        + "'src-id-index'='0',"
//                        + "'dst-id-index'='1',"
//                        + "'rank-id-index'='4',"
//                        + "'data-type'='edge'"
//                        + ")"
//                                   );
//
//        tableEnvironment.executeSql(
//                "INSERT INTO friend VALUES ('61', '62', 'aba', 'abcdefgh',"
//                        + " '1', '1111', '22222', '6412233', '2019-01-01',"
//                        + " '2019-01-01T12:12:12',"
//                        + " '435463424', 'false', '1.2', '1.0', '11:12:12', 'POINT(1 3)')"
//                                   ).await();
//
//        check(
//                Arrays.asList(
//                        rowOf(
//                                valueOf(61), valueOf(62), valueOf(1),
//                                valueOf("aba"), valueOf("abcdefgh"),
//                                valueOf(null),
//                                valueOf(1111), valueOf(22222), valueOf(6412233),
//                                dateOf(2019, 1, 1), dateTimeOf(2019, 1, 1, 4, 12, 12, 0),
//                                valueOf(435463424), valueOf(false), valueOf(1.2), valueOf(1.0),
//                                timeOf(3, 12, 12, 0), pointOf(1.0, 3.0)
//                             )
//                             ),
//                "MATCH ()-[r:friend]->(n:person)"
//                        + " WHERE id(n) IN [62]"
//                        + " RETURN src(r), dst(r), rank(r),"
//                        + " r.col1, r.col2, r.col3, r.col4, r.col5, r.col6, r.col7,"
//                        + " r.col8, r.col9, r.col10, r.col11, r.col12, r.col13, r.col14"
//                        + " LIMIT 1"
//             );
//    }
//
//    /**
//     * sink Nebula Graph Edge Data without rank with default INSERT mode
//     */
//    @Test
//    public void testSinkEdgeDataWithoutRank() throws ExecutionException, InterruptedException {
//        check(null, "INSERT vertex person() VALUES 61:()");
//        check(null, "INSERT vertex person() VALUES 89:()");
//
//        tableEnvironment.executeSql(
//                "CREATE TABLE friend_without_rank ("
//                        + "src STRING,"
//                        + "dst STRING,"
//                        + "col1 STRING,"
//                        + "col2 STRING,"
//                        + "col3 STRING,"
//                        + "col4 STRING,"
//                        + "col5 STRING,"
//                        + "col6 STRING,"
//                        + "col7 STRING,"
//                        + "col8 STRING,"
//                        + "col9 STRING,"
//                        + "col10 STRING,"
//                        + "col11 STRING,"
//                        + "col12 STRING,"
//                        + "col13 STRING,"
//                        + "col14 STRING"
//                        + ")"
//                        + "WITH ("
//                        + "'connector'='nebula',"
//                        + "'meta-address'='"
//                        + META_ADDRESS
//                        + "',"
//                        + "'graph-address'='"
//                        + GRAPH_ADDRESS
//                        + "',"
//                        + "'username'='"
//                        + USERNAME
//                        + "',"
//                        + "'password'='"
//                        + PASSWORD
//                        + "',"
//                        + "'graph-space'='flink_test',"
//                        + "'label-name'='friend',"
//                        + "'src-id-index'='0',"
//                        + "'dst-id-index'='1',"
//                        + "'data-type'='edge'"
//                        + ")"
//                                   );
//
//        tableEnvironment.executeSql(
//                "INSERT INTO friend_without_rank VALUES ('61', '89', 'aba', 'abcdefgh',"
//                        + " '1', '1111', '22222', '6412233', '2019-01-01',"
//                        + " '2019-01-01T12:12:12', '435463424', 'false', '1.2', '1.0',"
//                        + " '11:12:12', 'POINT(1 3)')"
//                                   ).await();
//
//        check(
//                Arrays.asList(
//                        rowOf(
//                                valueOf(61), valueOf(89), valueOf(0),
//                                valueOf("aba"), valueOf("abcdefgh"),
//                                valueOf(1), valueOf(1111), valueOf(22222), valueOf(6412233),
//                                dateOf(2019, 1, 1), dateTimeOf(2019, 1, 1, 4, 12, 12, 0),
//                                valueOf(435463424), valueOf(false), valueOf(1.2), valueOf(1.0),
//                                timeOf(3, 12, 12, 0), pointOf(1.0, 3.0)
//                             )
//                             ),
//                "MATCH ()-[r:friend]->(n:person)"
//                        + " WHERE id(n) IN [89]"
//                        + " RETURN src(r), dst(r), rank(r),"
//                        + " r.col1, r.col2, r.col3, r.col4, r.col5, r.col6, r.col7,"
//                        + " r.col8, r.col9, r.col10, r.col11, r.col12, r.col13, r.col14"
//                        + " LIMIT 10"
//             );
//    }
//}
