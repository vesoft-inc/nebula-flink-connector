/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.PropertyType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.MockData;
import org.apache.flink.connector.nebula.NebulaITTestBase;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.FailureHandlerEnum;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaVertexBatchExecutorTest extends NebulaITTestBase {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(NebulaVertexBatchExecutorTest.class);

    VertexExecutionOptions.ExecutionOptionBuilder builder = null;
    Map<String, Integer> schema = new HashMap<>();
    Row row1 = new Row(9);
    Row row2 = new Row(9);

    @BeforeClass
    public static void beforeAll() {
        initializeNebulaSession();
        initializeNebulaSchema(MockData.createIntSpace());
        initializeNebulaSchema(MockData.createStringSpace());
    }

    @AfterClass
    public static void afterAll() {
        closeNebulaSession();
    }

    @Before
    public void before() {
        builder = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setTag("person")
                .setIdIndex(0)
                .setFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6", "col7",
                        "col8"))
                .setPositions(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));

        schema.put("col1", PropertyType.STRING.getValue());
        schema.put("col2", PropertyType.FIXED_STRING.getValue());
        schema.put("col3", PropertyType.INT32.getValue());
        schema.put("col4", PropertyType.DOUBLE.getValue());
        schema.put("col5", PropertyType.DATE.getValue());
        schema.put("col6", PropertyType.DATETIME.getValue());
        schema.put("col7", PropertyType.TIME.getValue());
        schema.put("col8", PropertyType.TIMESTAMP.getValue());

        row1.setField(0, 1);
        row1.setField(1, "Tom");
        row1.setField(2, "Tom");
        row1.setField(3, 10);
        row1.setField(4, 1.0);
        row1.setField(5, "2021-01-01");
        row1.setField(6, "2021-01-01T12:00:00");
        row1.setField(7, "12:00:00");
        row1.setField(8, 372435234);

        row2.setField(0, 2);
        row2.setField(1, "Jina");
        row2.setField(2, "Jina");
        row2.setField(3, 20);
        row2.setField(4, 2.0);
        row2.setField(5, "2021-02-01");
        row2.setField(6, "2021-02-01T12:00:00");
        row2.setField(7, "15:00:00");
        row2.setField(8, 392435234);
    }

    /**
     * test addToBatch for INSERT write mode
     */
    @Test
    public void testAddToBatchWithInsert() {
        VertexExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setWriteMode(WriteModeEnum.INSERT)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.INT, schema);
        vertexBatchExecutor.addToBatch(row1);
    }

    /**
     * test addToBatch for INSERT write mode
     */
    @Test
    public void testAddToBatchWithInsertPolicy() {
        VertexExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.INSERT)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.INT, schema);
        vertexBatchExecutor.addToBatch(row1);
    }

    /**
     * test addToBatch for UPDATE write mode
     */
    @Test
    public void testAddToBatchWithUpdate() {
        VertexExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setWriteMode(WriteModeEnum.UPDATE)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.INT, schema);
        vertexBatchExecutor.addToBatch(row1);
    }

    /**
     * test addToBatch for UPDATE write mode
     */
    @Test
    public void testAddToBatchWithUpdatePolicy() {
        VertexExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.UPDATE)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.INT, schema);
        vertexBatchExecutor.addToBatch(row1);
    }

    /**
     * test addToBatch for DELETE write mode
     */
    @Test
    public void testAddToBatchWithDelete() {
        VertexExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setWriteMode(WriteModeEnum.DELETE)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.INT, schema);
        vertexBatchExecutor.addToBatch(row1);
    }

    /**
     * test addToBatch for DELETE write mode
     */
    @Test
    public void testAddToBatchWithDeletePolicy() {
        VertexExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.DELETE)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.INT, schema);
        vertexBatchExecutor.addToBatch(row1);
    }

    /**
     * test batch execute for int vid and insert mode
     */
    @Test
    public void testExecuteBatch() {
        VertexExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.INSERT)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.INT, schema);
        vertexBatchExecutor.addToBatch(row1);
        vertexBatchExecutor.addToBatch(row2);

        executeNGql("USE test_int");
        vertexBatchExecutor.executeBatch(session);
    }

    /**
     * test batch execute for int vid and UPDATE mode
     */
    @Test
    public void testExecuteBatchWithUpdate() {
        testExecuteBatch();
        VertexExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.UPDATE)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.INT, schema);
        vertexBatchExecutor.addToBatch(row1);
        vertexBatchExecutor.addToBatch(row2);

        executeNGql("USE test_int");
        vertexBatchExecutor.executeBatch(session);
    }

    /**
     * test batch execute for int vid and DELETE mode
     */
    @Test
    public void testExecuteBatchWithDelete() {
        VertexExecutionOptions options = builder.setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.DELETE)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.INT, schema);
        vertexBatchExecutor.addToBatch(row1);
        vertexBatchExecutor.addToBatch(row2);

        executeNGql("USE test_int");
        vertexBatchExecutor.executeBatch(session);
    }

    /**
     * test batch execute with invalid data and fail
     */
    @Test(expected = RuntimeException.class)
    public void testExecuteBatchFailInvalid() {
        VertexExecutionOptions options = builder.setGraphSpace("test_int")
                .setWriteMode(WriteModeEnum.INSERT)
                .setFailureHandler(FailureHandlerEnum.FAIL)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.INT, schema);
        Row row = Row.copy(row1);
        row.setField(3, "abc");
        vertexBatchExecutor.addToBatch(row);

        executeNGql("USE test_int");
        vertexBatchExecutor.executeBatch(session);
    }

    /**
     * test batch execute with invalid data and ignore
     */
    @Test
    public void testExecuteBatchIgnoreInvalid() {
        VertexExecutionOptions options = builder.setGraphSpace("test_int")
                .setWriteMode(WriteModeEnum.INSERT)
                .setFailureHandler(FailureHandlerEnum.IGNORE)
                .setMaxRetries(2)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.INT, schema);
        Row row = Row.copy(row1);
        row.setField(3, "abc");
        vertexBatchExecutor.addToBatch(row);

        executeNGql("USE test_int");
        vertexBatchExecutor.executeBatch(session);
    }

    /**
     * test batch execute for string vid and insert mode
     */
    @Test
    public void testExecuteBatchWithStringVidAndInsert() {
        VertexExecutionOptions options = builder
                .setGraphSpace("test_string")
                .setWriteMode(WriteModeEnum.INSERT)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.STRING, schema);
        vertexBatchExecutor.addToBatch(row1);
        vertexBatchExecutor.addToBatch(row2);

        executeNGql("USE test_string");
        vertexBatchExecutor.executeBatch(session);
    }

    /**
     * test batch execute for string vid and update mode
     */
    @Test
    public void testExecuteBatchWithStringVidAndUpdate() {
        testExecuteBatchWithStringVidAndInsert();
        VertexExecutionOptions options = builder
                .setGraphSpace("test_string")
                .setWriteMode(WriteModeEnum.UPDATE)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.STRING, schema);
        vertexBatchExecutor.addToBatch(row1);
        vertexBatchExecutor.addToBatch(row2);

        executeNGql("USE test_string");
        vertexBatchExecutor.executeBatch(session);
    }

    /**
     * test batch execute for string vid and DELETE mode
     */
    @Test
    public void testExecuteBatchWithStringVidAndDelete() {
        VertexExecutionOptions options = builder
                .setGraphSpace("test_string")
                .setWriteMode(WriteModeEnum.DELETE)
                .build();
        NebulaVertexBatchExecutor vertexBatchExecutor =
                new NebulaVertexBatchExecutor(options, VidTypeEnum.STRING, schema);
        vertexBatchExecutor.addToBatch(row1);
        vertexBatchExecutor.addToBatch(row2);

        executeNGql("USE test_string");
        vertexBatchExecutor.executeBatch(session);
    }

}
