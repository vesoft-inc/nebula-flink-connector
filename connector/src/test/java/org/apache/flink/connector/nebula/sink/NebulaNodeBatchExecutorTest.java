/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import static org.apache.flink.connector.nebula.TestConstant.sinkGraph;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.MockData;
import org.apache.flink.connector.nebula.NebulaITTestBase;
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.utils.NebulaNodeSchema;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaNodeBatchExecutorTest extends NebulaITTestBase {

    private static final Logger LOGGER = LoggerFactory.getLogger(NebulaNodeBatchExecutorTest.class);


    SinkNodeOptions.Builder builder    = null;
    Map<String, String>     schema     = new HashMap<>();
    NebulaNodeSchema        nodeSchema = new NebulaNodeSchema();
    Row                     row1       = Row.withNames();
    Row                     row2       = Row.withNames();

    @BeforeClass
    public static void beforeAll() {
        initializeNebulaClient();
        initializeNebulaSchema(MockData.createFlinkSinkGraphType());
        initializeNebulaSchema(MockData.createFlinkSinkGraph());


    }

    @AfterClass
    public static void afterAll() {
        closeGraphProvider();
    }

    @Before
    public void before() {
        schema.put("col1", "STRING");
        schema.put("col2", "STRING");
        schema.put("col3", "INT32");
        schema.put("col4", "DOUBLE");
        schema.put("col5", "DATE");
        schema.put("col6", "ZONED DATETIME");
        schema.put("col7", "ZONED TIME");
        schema.put("col8", "INT64");
        nodeSchema.setNodeTypeName("person");
        nodeSchema.setPkNames(Collections.singletonList("col1"));
        nodeSchema.setPropNames(Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                              "col6", "col7", "col8"));
        nodeSchema.setProperties(schema);

        builder = new SinkNodeOptions.Builder()
                .withGraphName(sinkGraph)
                .withNodeType("person")
                .withWriteMode(WriteModeEnum.INSERTREPLACE)
                .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6",
                                                "col7", "col8"))
                .withFlinkFields(Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"));


        row1.setField("c1", 1);
        row1.setField("c2", "Tom");
        row1.setField("c3", 10);
        row1.setField("c4", 1.0);
        row1.setField("c5", "2021-01-01");
        row1.setField("c6", "2021-01-01T12:00:00+08:00");
        row1.setField("c7", "12:00:00+08:00");
        row1.setField("c8", 372435234);

        row2.setField("c1", 2);
        row2.setField("c2", "Jina");
        row2.setField("c3", 20);
        row2.setField("c4", 2.0);
        row2.setField("c5", "2021-02-01");
        row2.setField("c6", "2021-02-01T12:00:00+08:00");
        row2.setField("c7", "15:00:00+08:00");
        row2.setField("c8", 392435234);
    }

    /**
     * test addToBatch for INSERT write mode
     */
    @Test
    public void testAddToBatchWithInsert() {
        SinkNodeOptions options = builder
                .withGraphName(sinkGraph)
                .withWriteMode(WriteModeEnum.INSERTIGNORE)
                .build();

        NebulaNodeBatchExecutor vertexBatchExecutor =
                new NebulaNodeBatchExecutor(options, nodeSchema);
        vertexBatchExecutor.addToBatch(row1);
        vertexBatchExecutor.addToBatch(row2);
        String statement = vertexBatchExecutor.executeBatch(graphProvider);
        assert (statement == null);
    }

    /**
     * test addToBatch for UPDATE write mode
     */
    @Test
    public void testAddToBatchWithUpdate() {
        SinkNodeOptions options = builder
                .withGraphName(sinkGraph)
                .withWriteMode(WriteModeEnum.UPDATE)
                .build();

        NebulaNodeBatchExecutor nodeBatchExecutor =
                new NebulaNodeBatchExecutor(options, nodeSchema);
        nodeBatchExecutor.addToBatch(row1);
        nodeBatchExecutor.addToBatch(row2);
        String statement = nodeBatchExecutor.executeBatch(graphProvider);
        assert (statement == null);
    }


    /**
     * test addToBatch for DELETE write mode
     */
    @Test
    public void testAddToBatchWithDelete() {
        SinkNodeOptions options = builder
                .withGraphName(sinkGraph)
                .withWriteMode(WriteModeEnum.DETACHDELETE)
                .build();

        NebulaNodeBatchExecutor vertexBatchExecutor =
                new NebulaNodeBatchExecutor(options, nodeSchema);
        vertexBatchExecutor.addToBatch(row1);
        vertexBatchExecutor.addToBatch(row2);

        String statement = vertexBatchExecutor.executeBatch(graphProvider);
        assert (statement == null);
    }

}
