/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import static org.apache.flink.connector.nebula.TestConstant.graphAddr;
import static org.apache.flink.connector.nebula.TestConstant.sourceGraph;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.flink.connector.nebula.MockData;
import org.apache.flink.connector.nebula.NebulaITTestBase;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.NebulaEdgeSchema;
import org.apache.flink.connector.nebula.utils.NebulaEdges;
import org.apache.flink.connector.nebula.utils.NebulaNode;
import org.apache.flink.connector.nebula.utils.NebulaNodeSchema;
import org.apache.flink.connector.nebula.utils.NebulaNodes;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
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

    @BeforeClass
    public static void beforeAll() {
        initializeNebulaClient();
        initializeNebulaSchema(MockData.createFlinkSourceGraphType());
        initializeNebulaSchema(MockData.createFlinkSourceGraph());
    }

    @AfterClass
    public static void afterAll() {
        closeGraphProvider();
    }

    @Before
    public void before() {
        tableEnvironment = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
    }

    /**
     * construct flink vertex data
     */
    private static List<Map<String, String>> constructVertexSourceData() {
        Map<String, String>       fields1 = new HashMap<String, String>();
        fields1.put("col1", "1");
        fields1.put("col2", "\"aba\"");
        fields1.put("col3", "\"abcdefgh\"");
        fields1.put("col4", "1");
        fields1.put("col5", "1111");
        fields1.put("col6", "22222");
        fields1.put("col7", "6412233");
        fields1.put("col8", "date(\"2019-01-01\")");
        fields1.put("col9", "local_datetime(\"2019-01-01T12:12:12\")");
        fields1.put("col10", "local_time(\"12:12:12\")");
        fields1.put("col11", "zoned_datetime(\"2019-01-01T12:12:12+01:00\")");
        fields1.put("col12", "zoned_time(\"12:12:12+01:00\")");
        fields1.put("col13", "false");

        Map<String, String> fields2 = new HashMap<String, String>();
        fields2.put("col1", "2");
        fields2.put("col2", "\"aba\"");
        fields2.put("col3", "\"abcdefgh\"");
        fields2.put("col4", "2");
        fields2.put("col5", "1111");
        fields2.put("col6", "22222");
        fields2.put("col7", "6412233");
        fields2.put("col8", "date(\"2020-01-01\")");
        fields2.put("col9", "local_datetime(\"2020-01-01T12:12:12\")");
        fields2.put("col10", "local_time(\"12:12:12\")");
        fields2.put("col11", "zoned_datetime(\"2020-01-01T12:12:12+01:00\")");
        fields2.put("col12", "zoned_time(\"12:12:12+01:00\")");
        fields2.put("col13", "true");


        Map<String, String> fields3 = new HashMap<String, String>();

        fields3.put("col1", "3");
        fields3.put("col2", "\"aba\"");
        fields3.put("col3", "\"abcdefgh\"");
        fields3.put("col4", "3");
        fields3.put("col5", "1111");
        fields3.put("col6", "22222");
        fields3.put("col7", "6412233");
        fields3.put("col8", "date(\"2021-01-01\")");
        fields3.put("col9", "local_datetime(\"2021-01-01T12:12:12\")");
        fields3.put("col10", "local_time(\"12:12:12\")");
        fields3.put("col11", "zoned_datetime(\"2021-01-01T12:12:12+01:00\")");
        fields3.put("col12", "zoned_time(\"12:12:12+01:00\")");
        fields3.put("col13", "false");


        Map<String, String> fields4 = new HashMap<String, String>();
        fields4.put("col1", "4");
        fields4.put("col2", "\"aba\"");
        fields4.put("col3", "\"abcdefgh\"");
        fields4.put("col4", "4");
        fields4.put("col5", "1111");
        fields4.put("col6", "22222");
        fields4.put("col7", "6412233");
        fields4.put("col8", "date(\"2021-01-01\")");
        fields4.put("col9", "local_datetime(\"2021-01-01T12:12:12\")");
        fields4.put("col10", "local_time(\"12:12:12\")");
        fields4.put("col11", "zoned_datetime(\"2021-01-01T12:12:12+01:00\")");
        fields4.put("col12", "zoned_time(\"12:12:12+01:00\")");
        fields4.put("col13", "true");

        List<Map<String, String>> persons = new ArrayList<>();
        persons.add(fields1);
        persons.add(fields2);
        persons.add(fields3);
        persons.add(fields4);
        return persons;
    }

    /**
     * construct flink edge data
     */
    private static List<Map<String, String>> constructEdgeSourceData(String src, String dst) {
        Map<String, String> srcMap = new HashMap<String, String>();
        srcMap.put("col1", "" + src);
        Map<String, String> dstMap = new HashMap<String, String>();
        dstMap.put("col1", "" + dst);
        Map<String, String> fields = new HashMap<>();
        fields.put("col1", "61");
        fields.put("col2", "\"aba\"");
        fields.put("col3", "\"abcdefgh\"");
        fields.put("col4", "1");
        fields.put("col5", "1111");
        fields.put("col6", "22222");
        fields.put("col7", "6412233");
        fields.put("col8", "date(\"2019-01-01\")");
        fields.put("col9", "local_datetime(\"2019-01-01T12:12:12\")");
        fields.put("col10", "local_time(\"12:12:12\")");
        fields.put("col11", "zoned_datetime(\"2019-01-01T12:12:12+01:00\")");
        fields.put("col12", "zoned_time(\"12:12:12+01:00\")");
        fields.put("col13", "false");

        List<Map<String, String>> friend = new ArrayList<>();
        friend.add(srcMap);
        friend.add(dstMap);
        friend.add(fields);
        return friend;
    }

    @Before
    public void insertData() throws Exception {
        executeNGql(getVertexInsertStatement());
        executeNGql(getEdgeInsertStatement());
    }

    //@Test
    public void testNodeSource() throws ExecutionException, InterruptedException {
        String sql = "CREATE TABLE `person` ("
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
                + " 'graph-address' = '" + graphAddr + "',"
                + " 'username' = 'root',"
                + " 'password' = 'NebulaGraph01',"
                + " 'data-type' = 'node',"
                + " 'graph-name' = 'flink_source_test',"
                + " 'label-name' = 'person'"
                + ")";
        tableEnvironment.executeSql(sql);


        Table table = tableEnvironment.sqlQuery("SELECT * FROM `person`");
        table.execute().await();
    }

    //@Test
    public void testEdgeSource() throws ExecutionException, InterruptedException {
        String sql = "CREATE TABLE `friend` ("
                + " sid BIGINT,"
                + " did BIGINT,"
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
                + " 'graph-address' = '" + graphAddr + "',"
                + " 'username' = 'root',"
                + " 'password' = 'nebula',"
                + " 'graph-name' = 'flink_source_test',"
                + " 'label-name' = 'friend',"
                + " 'data-type'='edge',"
                + " 'src-id-index'='0',"
                + " 'dst-id-index'='1',"
                + " 'rank-id-index'='2'"
                + ")";
        tableEnvironment.executeSql(sql);

        sql = "CREATE TABLE `friend_sink` ("
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
                + ")";
        tableEnvironment.executeSql(sql);

        Table table = tableEnvironment.sqlQuery("SELECT * FROM `friend`");
        table.executeInsert("`friend_sink`").await();
    }

    private String getVertexInsertStatement() {
        List<Map<String, String>> persons = constructVertexSourceData();
        List<NebulaNode>          nodes   = new ArrayList<>();
        for (Map<String, String> person : persons) {
            nodes.add(new NebulaNode(person));
        }

        Map<String, String> schema = new HashMap<>();
        schema.put("col1", "INT64");
        schema.put("col2", "STRING");
        schema.put("col3", "STRING");
        schema.put("col4", "INT8");
        schema.put("col5", "INT16");
        schema.put("col6", "INT32");
        schema.put("col7", "INT64");
        schema.put("col8", "DATE");
        schema.put("col9", "LOCAL DATETIME");
        schema.put("col10", "LOCAL TIME");
        schema.put("col11", "ZONED DATETIME");
        schema.put("col12", "ZONED TIME");
        schema.put("col13", "BOOL");

        NebulaNodeSchema nodeSchema = new NebulaNodeSchema();
        nodeSchema.setNodeTypeName("person");
        nodeSchema.setPkNames(Collections.singletonList("col1"));
        nodeSchema.setPropNames(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6",
                                              "col7", "col8", "col9", "col10", "col11", "col12",
                                              "col13"));
        nodeSchema.setProperties(schema);
        NebulaNodes nebulaNodes = new NebulaNodes(nodeSchema, nodes);

        List<String> nebulaFields = Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                                  "col6", "col7", "col8", "col9", "col10",
                                                  "col11", "col12", "col13");
        List<String> flinkFields = Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6", "c7",
                                                 "c8", "c9", "c10", "c11", "c12", "c13");
        return nebulaNodes.getInsertStatement(sourceGraph,
                                              WriteModeEnum.INSERTREPLACE,
                                              flinkFields,
                                              nebulaFields);
    }

    private String getEdgeInsertStatement() {
        List<Map<String, String>> friend1 = constructEdgeSourceData("1", "2");
        List<Map<String, String>> friend2 = constructEdgeSourceData("3", "4");
        List<NebulaEdge>          edges   = new ArrayList<>();
        edges.add(new NebulaEdge(friend1.get(0), friend1.get(1), friend1.get(2)));
        edges.add(new NebulaEdge(friend2.get(0), friend2.get(1), friend2.get(2)));

        Map<String, String> schema = new HashMap<>();
        schema.put("col1", "INT64");
        schema.put("col2", "STRING");
        schema.put("col3", "STRING");
        schema.put("col4", "INT8");
        schema.put("col5", "INT16");
        schema.put("col6", "INT32");
        schema.put("col7", "INT64");
        schema.put("col8", "DATE");
        schema.put("col9", "LOCAL DATETIME");
        schema.put("col10", "LOCAL TIME");
        schema.put("col11", "ZONED DATETIME");
        schema.put("col12", "ZONED TIME");
        schema.put("col13", "BOOL");
        NebulaEdgeSchema edgeSchema = new NebulaEdgeSchema();
        edgeSchema.setEdgeTypeName("friend");
        edgeSchema.setSrcNodeTypeName("person");
        edgeSchema.setDstNodeTypeName("person");

        Map<String, String> srcPkDataType = new HashMap<>();
        srcPkDataType.put("col1", "INT64");
        edgeSchema.setSrcPkDataTypeMap(srcPkDataType);
        Map<String, String> dstPkDataType = new HashMap<>();
        dstPkDataType.put("col1", "INT64");
        edgeSchema.setDstPkDataTypeMap(dstPkDataType);
        edgeSchema.setProperties(schema);
        NebulaEdges nebulaEdges = new NebulaEdges(edgeSchema, edges);

        List<String> nebulaFields = Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                                  "col6", "col7", "col8", "col9", "col10", "col11",
                                                  "col12", "col13");
        List<String> flinkFields = Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8",
                                                 "c9", "c10", "c11", "c12", "c13");
        List<String> nebulaSrcPks = Arrays.asList("col1");
        List<String> nebulaDstPks = Arrays.asList("col1");
        List<String> flinkSrc     = Arrays.asList("src");
        List<String> flinkDst     = Arrays.asList("dst");
        return nebulaEdges.getInsertStatement(sourceGraph, WriteModeEnum.INSERTREPLACE,
                                              flinkSrc, nebulaSrcPks, flinkDst, nebulaDstPks,
                                              flinkFields, nebulaFields);
    }
}
