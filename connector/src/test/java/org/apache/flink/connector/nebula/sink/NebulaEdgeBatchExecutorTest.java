/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import com.vesoft.nebula.meta.PropertyType;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.connector.nebula.MockData;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaEdgeBatchExecutorTest {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(NebulaEdgeBatchExecutorTest.class);

    String ip = "127.0.0.1";
    EdgeExecutionOptions.ExecutionOptionBuilder builder = null;
    Map<String, Integer> schema = new HashMap<>();
    Row row1 = new Row(10);
    Row row2 = new Row(10);

    Session session = null;

    @Before
    public void before() {
        MockData.mockSchema();
        builder = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setEdge("friend")
                .setSrcIndex(0)
                .setDstIndex(1)
                .setFields(Arrays.asList("col1", "col2", "col3", "col4", "col5", "col6", "col7",
                        "col8"))
                .setPositions(Arrays.asList(2, 3, 4, 5, 6, 7, 8, 9));

        schema.put("col1", PropertyType.STRING.getValue());
        schema.put("col2", PropertyType.FIXED_STRING.getValue());
        schema.put("col3", PropertyType.INT32.getValue());
        schema.put("col4", PropertyType.DOUBLE.getValue());
        schema.put("col5", PropertyType.DATE.getValue());
        schema.put("col6", PropertyType.DATETIME.getValue());
        schema.put("col7", PropertyType.TIME.getValue());
        schema.put("col8", PropertyType.TIMESTAMP.getValue());

        row1.setField(0, 1);
        row1.setField(1, 2);
        row1.setField(2, "Tom");
        row1.setField(3, "Tom");
        row1.setField(4, 10);
        row1.setField(5, 1.0);
        row1.setField(6, "2021-01-01");
        row1.setField(7, "2021-01-01T12:00:00");
        row1.setField(8, "12:00:00");
        row1.setField(9, 372435234);

        row2.setField(0, 2);
        row2.setField(1, 3);
        row2.setField(2, "Jina");
        row2.setField(3, "Jina");
        row2.setField(4, 20);
        row2.setField(5, 2.0);
        row2.setField(6, "2021-02-01");
        row2.setField(7, "2021-02-01T12:00:00");
        row2.setField(8, "15:00:00");
        row2.setField(9, 392435234);

        // get Session
        NebulaPoolConfig poolConfig = new NebulaPoolConfig();
        NebulaPool pool = new NebulaPool();

        try {
            pool.init(Arrays.asList(new HostAddress(ip, 9669)), poolConfig);
            session = pool.getSession("root", "nebula", true);
        } catch (Exception e) {
            LOGGER.error("init nebula pool error, ", e);
            assert (false);
        }
    }

    /**
     * test addToBatch for INSERT write mode
     */
    @Test
    public void testAddToBatchWithInsert() {
        ExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setWriteMode(WriteModeEnum.INSERT)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.INT, schema);
        edgeBatchExecutor.addToBatch(row1);
    }

    /**
     * test addToBatch for INSERT write mode
     */
    @Test
    public void testAddToBatchWithInsertPolicy() {
        ExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.INSERT)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.INT, schema);
        edgeBatchExecutor.addToBatch(row1);
    }

    /**
     * test addToBatch for UPDATE write mode
     */
    @Test
    public void testAddToBatchWithUpdate() {
        ExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setWriteMode(WriteModeEnum.UPDATE)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.INT, schema);
        edgeBatchExecutor.addToBatch(row1);
    }

    /**
     * test addToBatch for UPDATE write mode
     */
    @Test
    public void testAddToBatchWithUpdatePolicy() {
        ExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.UPDATE)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.INT, schema);
        edgeBatchExecutor.addToBatch(row1);
    }

    /**
     * test addToBatch for DELETE write mode
     */
    @Test
    public void testAddToBatchWithDelete() {
        ExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setWriteMode(WriteModeEnum.DELETE)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.INT, schema);
        edgeBatchExecutor.addToBatch(row1);
    }

    /**
     * test addToBatch for DELETE write mode
     */
    @Test
    public void testAddToBatchWithDeletePolicy() {
        ExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.DELETE)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.INT, schema);
        edgeBatchExecutor.addToBatch(row1);
    }

    /**
     * test batch execute for int vid and insert mode
     */
    @Test
    public void testExecuteBatch() {
        ExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.INSERT)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.INT, schema);
        edgeBatchExecutor.addToBatch(row1);
        edgeBatchExecutor.addToBatch(row2);

        ResultSet resultSet = null;
        try {
            resultSet = session.execute("USE test_int");
        } catch (IOErrorException e) {
            LOGGER.error("switch space error,", e);
            assert (false);
        }
        if (!resultSet.isSucceeded()) {
            LOGGER.error("switch space failed,{}", resultSet.getErrorMessage());
            assert (false);
        }

        String statement = edgeBatchExecutor.executeBatch(session);
        assert (statement == null);
    }

    /**
     * test batch exeucte for int vid and UPDATE mode
     */
    @Test
    public void testExecuteBatchWithUpdate() {
        ExecutionOptions options = builder
                .setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.UPDATE)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.INT, schema);
        edgeBatchExecutor.addToBatch(row1);
        edgeBatchExecutor.addToBatch(row2);

        ResultSet resultSet = null;
        try {
            resultSet = session.execute("USE test_int");
        } catch (IOErrorException e) {
            LOGGER.error("switch space error,", e);
            assert (false);
        }
        if (!resultSet.isSucceeded()) {
            LOGGER.error("switch space failed,{}", resultSet.getErrorMessage());
            assert (false);
        }

        String statement = edgeBatchExecutor.executeBatch(session);
        assert (statement == null);
    }

    /**
     * test batch exeucte for int vid and DELETE mode
     */
    @Test
    public void testExecuteBatchWithDelete() {
        ExecutionOptions options = builder.setGraphSpace("test_int")
                .setPolicy("HASH")
                .setWriteMode(WriteModeEnum.DELETE)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.INT, schema);
        edgeBatchExecutor.addToBatch(row1);
        edgeBatchExecutor.addToBatch(row2);

        ResultSet resultSet = null;
        try {
            resultSet = session.execute("USE test_int");
        } catch (IOErrorException e) {
            LOGGER.error("switch space error,", e);
            assert (false);
        }
        if (!resultSet.isSucceeded()) {
            LOGGER.error("switch space failed,{}", resultSet.getErrorMessage());
            assert (false);
        }

        String statement = edgeBatchExecutor.executeBatch(session);
        assert (statement == null);
    }

    /**
     * test batch exeucte for string vid and insert mode
     */
    @Test
    public void testExecuteBatchWithStringVidAndInsert() {
        ExecutionOptions options = builder
                .setGraphSpace("test_string")
                .setWriteMode(WriteModeEnum.INSERT)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.STRING, schema);
        edgeBatchExecutor.addToBatch(row1);
        edgeBatchExecutor.addToBatch(row2);

        ResultSet resultSet = null;
        try {
            resultSet = session.execute("USE test_string");
        } catch (IOErrorException e) {
            LOGGER.error("switch space error,", e);
            assert (false);
        }
        if (!resultSet.isSucceeded()) {
            LOGGER.error("switch space failed,{}", resultSet.getErrorMessage());
            assert (false);
        }

        String statement = edgeBatchExecutor.executeBatch(session);
        assert (statement == null);
    }

    /**
     * test batch execute for string vid and update mode
     */
    @Test
    public void testExecuteBatchWithStringVidAndUpdate() {
        ExecutionOptions options = builder
                .setGraphSpace("test_string")
                .setWriteMode(WriteModeEnum.UPDATE)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.STRING, schema);
        edgeBatchExecutor.addToBatch(row1);
        edgeBatchExecutor.addToBatch(row2);

        ResultSet resultSet = null;
        try {
            resultSet = session.execute("USE test_string");
        } catch (IOErrorException e) {
            LOGGER.error("switch space error,", e);
            assert (false);
        }
        if (!resultSet.isSucceeded()) {
            LOGGER.error("switch space failed,{}", resultSet.getErrorMessage());
            assert (false);
        }

        String statement = edgeBatchExecutor.executeBatch(session);
        assert (statement == null);
    }

    /**
     * test batch execute for string vid and DELETE mode
     */
    @Test
    public void testExecuteBatchWithStringVidAndDelete() {
        ExecutionOptions options = builder
                .setGraphSpace("test_string")
                .setWriteMode(WriteModeEnum.DELETE)
                .builder();
        NebulaEdgeBatchExecutor<Row> edgeBatchExecutor =
                new NebulaEdgeBatchExecutor<>(options, VidTypeEnum.STRING, schema);
        edgeBatchExecutor.addToBatch(row1);
        edgeBatchExecutor.addToBatch(row2);

        ResultSet resultSet = null;
        try {
            resultSet = session.execute("USE test_string");
        } catch (IOErrorException e) {
            LOGGER.error("switch space error,", e);
            assert (false);
        }
        if (!resultSet.isSucceeded()) {
            LOGGER.error("switch space failed,{}", resultSet.getErrorMessage());
            assert (false);
        }

        String statement = edgeBatchExecutor.executeBatch(session);
        assert (statement == null);
    }

}
