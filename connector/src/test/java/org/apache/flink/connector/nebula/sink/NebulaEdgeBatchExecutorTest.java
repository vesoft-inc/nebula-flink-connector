/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import static org.apache.flink.connector.nebula.TestConstant.sinkGraph;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.MockData;
import org.apache.flink.connector.nebula.NebulaITTestBase;
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdgeSchema;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaEdgeBatchExecutorTest extends NebulaITTestBase {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(NebulaEdgeBatchExecutorTest.class);

    SinkEdgeOptions.Builder builder = null;
    Map<String, String>     schema  = new HashMap<>();
    Row                     row1    = Row.withNames();
    Row                     row2    = Row.withNames();

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
        builder = SinkEdgeOptions.builder()
                .withGraphName(sinkGraph)
                .withEdgeType("friend")
                .withNebulaDstPks(Arrays.asList("col1"))
                .withNebulaSrcPks(Arrays.asList("col1"))
                .withFlinkSrcPkFields(Arrays.asList("src"))
                .withFlinkDstPkFields(Arrays.asList("dst"))
                .withNebulaFields(Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                                "col6", "col7", "col8"))
                .withFlinkFields(Arrays.asList("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"));


        schema.put("col1", "STRING");
        schema.put("col2", "STRING");
        schema.put("col3", "INT32");
        schema.put("col4", "DOUBLE");
        schema.put("col5", "DATE");
        schema.put("col6", "ZONED DATETIME");
        schema.put("col7", "ZONED TIME");
        schema.put("col8", "INT64");

        row1.setField("src", 1);
        row1.setField("dst", 2);
        row1.setField("c1", 1);
        row1.setField("c2", "Tom");
        row1.setField("c3", 10);
        row1.setField("c4", 1.0);
        row1.setField("c5", "2021-01-01");
        row1.setField("c6", "2021-01-01T12:00:00+08:00");
        row1.setField("c7", "12:00:00+08:00");
        row1.setField("c8", 372435234);

        row2.setField("src", 3);
        row2.setField("dst", 4);
        row2.setField("c1", 2);
        row2.setField("c2", "Jina");
        row2.setField("c3", 20);
        row2.setField("c4", 2.0);
        row2.setField("c5", "2021-02-01");
        row2.setField("c6", "2021-02-01T12:00:00+08:00");
        row2.setField("c7", "15:00:00+08:00");
        row2.setField("c8", 392435234);

        // insert nodes with pk 1,2,3,4
        try {
            graphProvider.execute("use " + sinkGraph
                                          + " INSERT OR IGNORE(@person{col1:\"1\"}),"
                                          + "(@person{col1:\"2\"}),"
                                          + "(@person{col1:\"3\"}),"
                                          + "(@person{col1:\"4\"})");
        } catch (Exception e) {
            Assert.fail("pre-insert nodes failed: " + e.getMessage());
        }
    }

    /**
     * test addToBatch for INSERT write mode
     */
    @Test
    public void testAddToBatchWithInsert() {
        SinkEdgeOptions options = builder
                .withGraphName(sinkGraph)
                .withWriteMode(WriteModeEnum.INSERTREPLACE)
                .build();
        NebulaEdgeSchema edgeSchema = new NebulaEdgeSchema();
        edgeSchema.setEdgeTypeName("friend");
        edgeSchema.setSrcNodeTypeName("person");
        edgeSchema.setSrcPkDataTypeMap(new HashMap<String, String>() {
            {
                put("col1", "STRING");
            }
        });
        edgeSchema.setDstNodeTypeName("person");
        edgeSchema.setDstPkDataTypeMap(new HashMap<String, String>() {
            {
                put("col1", "STRING");
            }
        });
        edgeSchema.setPropNames(Arrays.asList("col1", "col2", "col3", "col4", "col5",
                                              "col6", "col7", "col8"));
        edgeSchema.setProperties(schema);
        NebulaEdgeBatchExecutor edgeBatchExecutor =
                new NebulaEdgeBatchExecutor(options, edgeSchema);
        edgeBatchExecutor.addToBatch(row1);
        edgeBatchExecutor.addToBatch(row2);
        String failedStmt = edgeBatchExecutor.executeBatch(graphProvider);
        assert (failedStmt == null);
    }
}
