/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import com.vesoft.nebula.client.graph.exception.IOErrorException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.flink.connector.nebula.MockData;
import org.apache.flink.connector.nebula.NebulaITTestBase;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.NebulaEdges;
import org.apache.flink.connector.nebula.utils.NebulaVertex;
import org.apache.flink.connector.nebula.utils.NebulaVertices;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AbstractNebulaInputFormatITTest extends NebulaITTestBase {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractNebulaInputFormatITTest.class);

    private static TableEnvironment tableEnvironment;
    private final String[] colNames = {"col1", "col2", "col3", "col4", "col5", "col6", "col7",
                                       "col8", "col9", "col10", "col11", "col12", "col13", "col14"};

    @BeforeClass
    public static void beforeAll() {
        initializeNebulaSession();
        initializeNebulaSchema(MockData.createFlinkTestSpace());
    }

    @AfterClass
    public static void afterAll() {
        closeNebulaSession();
    }

    @Before
    public void before() {
        tableEnvironment = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    /**
     * construct flink vertex data
     */
    private static List<List<String>> constructVertexSourceData() {
        List<List<String>> persons = new ArrayList<>();
        List<String> fields1 = Arrays.asList("61", "\"aba\"", "\"abcdefgh\"", "22", "1111", "22222",
                "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"POINT(1 3)\")");
        List<String> fields2 = Arrays.asList("62", "\"aba\"", "\"abcdefgh\"", "1", "1111", "22222",
                "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"POINT(1 3)\")");
        List<String> fields3 = Arrays.asList("63", "\"aba\"", "\"abcdefgh\"", "1", "1111", "22222",
                "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"POINT(1 3)\")");
        List<String> fields4 = Arrays.asList("64", "\"aba\"", "\"abcdefgh\"", "1", "1111", "22222",
                "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"LINESTRING(1 3,2 4)\")");
        List<String> fields5 = Arrays.asList("65", "\"aba\"", "\"abcdefgh\"", "1", "1111", "22222",
                "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"LINESTRING(1 3,2 4)\")");
        List<String> fields6 = Arrays.asList("66", "\"aba\"", "\"abcdefgh\"", "1", "1111", "22222",
                "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"LINESTRING(1 3,2 4)\")");
        List<String> fields7 = Arrays.asList("67", "\"李四\"", "\"abcdefgh\"", "1", "1111", "22222",
                "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "true", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"polygon((0 1,1 2,2 3,0 1))\")");
        List<String> fields8 = Arrays.asList("68", "\"aba\"", "\"张三\"", "1", "1111", "22222",
                "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "true", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"POLYGON((0 1,1 2,2 3,0 1))\")");
        persons.add(fields1);
        persons.add(fields2);
        persons.add(fields3);
        persons.add(fields4);
        persons.add(fields5);
        persons.add(fields6);
        persons.add(fields7);
        persons.add(fields8);
        return persons;
    }

    /**
     * construct flink edge data
     */
    private static List<List<String>> constructEdgeSourceData() {
        List<List<String>> friends = new ArrayList<>();
        List<String> fields1 = Arrays.asList("61", "62", "\"aba\"", "\"abcdefgh\"", "22", "1111",
                "22222", "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"POINT(1 3)\")");
        List<String> fields2 = Arrays.asList("62", "63", "\"aba\"", "\"abcdefgh\"", "1", "1111",
                "22222", "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"POINT(1 3)\")");
        List<String> fields3 = Arrays.asList("63", "64", "\"aba\"", "\"abcdefgh\"", "1", "1111",
                "22222", "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"POINT(1 3)\")");
        List<String> fields4 = Arrays.asList("64", "65", "\"aba\"", "\"abcdefgh\"", "1", "1111",
                "22222", "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"LINESTRING(1 3,2 4)\")");
        List<String> fields5 = Arrays.asList("65", "66", "\"aba\"", "\"abcdefgh\"", "1", "1111",
                "22222", "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"LINESTRING(1 3,2 4)\")");
        List<String> fields6 = Arrays.asList("66", "67", "\"aba\"", "\"abcdefgh\"", "1", "1111",
                "22222", "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "false", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"LINESTRING(1 3,2 4)\")");
        List<String> fields7 = Arrays.asList("67", "68", "\"李四\"", "\"abcdefgh\"", "1", "1111",
                "22222", "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "true", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"polygon((0 1,1 2,2 3,0 1))\")");
        List<String> fields8 = Arrays.asList("68", "61", "\"aba\"", "\"张三\"", "1", "1111", "22222",
                "6412233", "date(\"2019-01-01\")", "datetime(\"2019-01-01T12:12:12\")",
                "435463424", "true", "1.2", "1.0", "time(\"11:12:12\")",
                "ST_GeogFromText(\"POLYGON((0 1,1 2,2 3,0 1))\")");
        friends.add(fields1);
        friends.add(fields2);
        friends.add(fields3);
        friends.add(fields4);
        friends.add(fields5);
        friends.add(fields6);
        friends.add(fields7);
        friends.add(fields8);
        return friends;
    }

    @Before
    public void insertData() throws IOErrorException {
        executeNGql(getVertexInsertStatement());
        executeNGql(getEdgeInsertStatement());
    }

    @Test
    public void testVertexSource() throws ExecutionException, InterruptedException {
        tableEnvironment.executeSql("CREATE TABLE `person` ("
                + " vid BIGINT,"
                + " col1 STRING,"
                + " col2 STRING,"
                + " col3 BIGINT,"
                + " col4 BIGINT,"
                + " col5 BIGINT,"
                + " col6 BIGINT,"
                + " col7 DATE,"
                + " col8 TIMESTAMP,"
                + " col9 BIGINT,"
                + " col10 BOOLEAN,"
                + " col11 DOUBLE,"
                + " col12 DOUBLE,"
                + " col13 TIME,"
                + " col14 STRING"
                + ") WITH ("
                + " 'connector' = 'nebula',"
                + " 'meta-address' = '" + META_ADDRESS + "',"
                + " 'graph-address' = '" + GRAPH_ADDRESS + "',"
                + " 'username' = 'root',"
                + " 'password' = 'nebula',"
                + " 'data-type' = 'vertex',"
                + " 'graph-space' = 'flink_test',"
                + " 'label-name' = 'person'"
                + ")"
        );

        tableEnvironment.executeSql("CREATE TABLE `person_sink` ("
                + " vid BIGINT,"
                + " col1 STRING,"
                + " col2 STRING,"
                + " col3 BIGINT,"
                + " col4 BIGINT,"
                + " col5 BIGINT,"
                + " col6 BIGINT,"
                + " col7 DATE,"
                + " col8 TIMESTAMP,"
                + " col9 BIGINT,"
                + " col10 BOOLEAN,"
                + " col11 DOUBLE,"
                + " col12 DOUBLE,"
                + " col13 TIME,"
                + " col14 STRING"
                + ") WITH ("
                + " 'connector' = 'print'"
                + ")"
        );

        Table table = tableEnvironment.sqlQuery("SELECT * FROM `person`");
        table.executeInsert("`person_sink`").await();
    }

    @Test
    public void testEdgeSource() throws ExecutionException, InterruptedException {
        tableEnvironment.executeSql("CREATE TABLE `friend` ("
                + " sid BIGINT,"
                + " did BIGINT,"
                + " rid BIGINT,"
                + " col1 STRING,"
                + " col2 STRING,"
                + " col3 BIGINT,"
                + " col4 BIGINT,"
                + " col5 BIGINT,"
                + " col6 BIGINT,"
                + " col7 DATE,"
                + " col8 TIMESTAMP,"
                + " col9 BIGINT,"
                + " col10 BOOLEAN,"
                + " col11 DOUBLE,"
                + " col12 DOUBLE,"
                + " col13 TIME,"
                + " col14 STRING"
                + ") WITH ("
                + " 'connector' = 'nebula',"
                + " 'meta-address' = '" + META_ADDRESS + "',"
                + " 'graph-address' = '" + GRAPH_ADDRESS + "',"
                + " 'username' = 'root',"
                + " 'password' = 'nebula',"
                + " 'graph-space' = 'flink_test',"
                + " 'label-name' = 'friend',"
                + " 'data-type'='edge',"
                + " 'src-id-index'='0',"
                + " 'dst-id-index'='1',"
                + " 'rank-id-index'='2'"
                + ")"
        );

        tableEnvironment.executeSql("CREATE TABLE `friend_sink` ("
                + " sid BIGINT,"
                + " did BIGINT,"
                + " rid BIGINT,"
                + " col1 STRING,"
                + " col2 STRING,"
                + " col3 BIGINT,"
                + " col4 BIGINT,"
                + " col5 BIGINT,"
                + " col6 BIGINT,"
                + " col7 DATE,"
                + " col8 TIMESTAMP,"
                + " col9 BIGINT,"
                + " col10 BOOLEAN,"
                + " col11 DOUBLE,"
                + " col12 DOUBLE,"
                + " col13 TIME,"
                + " col14 STRING"
                + ") WITH ("
                + " 'connector' = 'print'"
                + ")"
        );

        Table table = tableEnvironment.sqlQuery("SELECT * FROM `friend`");
        table.executeInsert("`friend_sink`").await();
    }

    private String getVertexInsertStatement() {
        List<List<String>> persons = constructVertexSourceData();
        List<NebulaVertex> vertices = new ArrayList<>();
        for (List<String> person : persons) {
            vertices.add(new NebulaVertex(
                    person.get(0), person.subList(1, person.size())));
        }
        NebulaVertices nebulaVertices = new NebulaVertices(
                "person",
                Arrays.asList(colNames),
                vertices,
                null
        );
        return nebulaVertices.getInsertStatement();
    }

    private String getEdgeInsertStatement() {
        List<List<String>> friends = constructEdgeSourceData();
        List<NebulaEdge> edges = new ArrayList<>();
        for (List<String> friend : friends) {
            edges.add(new NebulaEdge(
                    friend.get(0), friend.get(1), 0L, friend.subList(2, friend.size())));
        }
        NebulaEdges nebulaEdges = new NebulaEdges(
                "friend",
                Arrays.asList(colNames),
                edges,
                null,
                null
        );
        return nebulaEdges.getInsertStatement();
    }
}
