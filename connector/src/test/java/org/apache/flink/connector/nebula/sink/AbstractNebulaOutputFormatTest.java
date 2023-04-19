/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import static org.apache.flink.connector.nebula.NebulaValueUtils.rowOf;
import static org.apache.flink.connector.nebula.NebulaValueUtils.valueOf;

import java.io.IOException;
import java.util.Arrays;
import org.apache.flink.connector.nebula.MockData;
import org.apache.flink.connector.nebula.NebulaITTestBase;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.FailureHandlerEnum;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractNebulaOutputFormatTest extends NebulaITTestBase {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractNebulaOutputFormatTest.class);

    private NebulaGraphConnectionProvider graphProvider;
    private NebulaMetaConnectionProvider metaProvider;
    private VertexExecutionOptions.ExecutionOptionBuilder vertexExecutionOptionBuilder = null;
    private EdgeExecutionOptions.ExecutionOptionBuilder edgeExecutionOptionBuilder = null;

    @BeforeClass
    public static void beforeAll() {
        initializeNebulaSession();
        initializeNebulaSchema(MockData.createFlinkSinkSpace());
    }

    @AfterClass
    public static void afterAll() {
        closeNebulaSession();
    }

    @Before
    public void before() {
        NebulaClientOptions clientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setMetaAddress(META_ADDRESS)
                .setGraphAddress(GRAPH_ADDRESS)
                .build();
        graphProvider =
                new NebulaGraphConnectionProvider(clientOptions);
        metaProvider = new NebulaMetaConnectionProvider(clientOptions);
        vertexExecutionOptionBuilder = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flink_sink")
                .setTag("player")
                .setFields(Arrays.asList("name", "age"))
                .setPositions(Arrays.asList(1, 2))
                .setIdIndex(0)
                .setBatchSize(1);
        edgeExecutionOptionBuilder = new EdgeExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flink_sink")
                .setEdge("follow")
                .setFields(Arrays.asList("degree"))
                .setPositions(Arrays.asList(2))
                .setSrcIndex(0)
                .setDstIndex(1)
                .setBatchSize(1);
    }

    @SafeVarargs
    private static <T> void writeRecords(
            NebulaBatchOutputFormat<T, ?> outputFormat, T... row) throws IOException {
        outputFormat.open(1, 2);
        try {
            for (T r : row) {
                outputFormat.writeRecord(r);
            }
        } finally {
            outputFormat.close();
        }
    }

    @Test
    public void testWriteVertex() throws IOException {
        final VertexExecutionOptions options = vertexExecutionOptionBuilder.build();
        final NebulaVertexBatchOutputFormat outputFormat =
                new NebulaVertexBatchOutputFormat(graphProvider, metaProvider, options);

        final Row row = new Row(3);
        row.setField(0, "111");
        row.setField(1, "jena");
        row.setField(2, 12);

        writeRecords(outputFormat, row);
        executeNGql("USE flink_sink");
        check(
                Arrays.asList(rowOf(valueOf(12))),
                "MATCH (n) WHERE id(n) == \"111\" RETURN n.player.age"
        );
    }

    @Test(expected = IOException.class)
    public void testWriteVertexFailInvalid() throws IOException {
        final VertexExecutionOptions options = vertexExecutionOptionBuilder
                .setWriteMode(WriteModeEnum.INSERT)
                .setFailureHandler(FailureHandlerEnum.FAIL)
                .setMaxRetries(2)
                .setRetryDelayMs(100)
                .build();
        final NebulaVertexBatchOutputFormat outputFormat =
                new NebulaVertexBatchOutputFormat(graphProvider, metaProvider, options);

        final Row row = new Row(3);
        row.setField(0, "222");
        row.setField(1, "jena");
        row.setField(2, "abc");

        writeRecords(outputFormat, row);
    }

    @Test
    public void testWriteVertexIgnoreInvalid() throws IOException {
        final VertexExecutionOptions options = vertexExecutionOptionBuilder
                .setWriteMode(WriteModeEnum.INSERT)
                .setFailureHandler(FailureHandlerEnum.IGNORE)
                .setMaxRetries(2)
                .setRetryDelayMs(100)
                .build();
        final NebulaVertexBatchOutputFormat outputFormat =
                new NebulaVertexBatchOutputFormat(graphProvider, metaProvider, options);

        final Row row1 = new Row(3);
        row1.setField(0, "222");
        row1.setField(1, "jena");
        row1.setField(2, "abc");
        final Row row2 = new Row(3);
        row2.setField(0, "222");
        row2.setField(1, "jena");
        row2.setField(2, 15);

        writeRecords(outputFormat, row1, row2);
        executeNGql("USE flink_sink");
        check(
                Arrays.asList(rowOf(valueOf(15))),
                "MATCH (n) WHERE id(n) == \"222\" RETURN n.player.age"
        );
    }

    @Test
    public void testWriteEdge() throws IOException {
        final EdgeExecutionOptions options = edgeExecutionOptionBuilder.build();
        final NebulaEdgeBatchOutputFormat outputFormat =
                new NebulaEdgeBatchOutputFormat(graphProvider, metaProvider, options);

        final Row row = new Row(3);
        row.setField(0, "111");
        row.setField(1, "333");
        row.setField(2, 20);

        writeRecords(outputFormat, row);
        executeNGql("USE flink_sink");
        check(
                Arrays.asList(rowOf(valueOf(20))),
                "GO FROM \"111\" OVER follow YIELD properties(edge).degree"
        );
    }

    @Test(expected = IOException.class)
    public void testWriteEdgeFailInvalid() throws IOException {
        final EdgeExecutionOptions options = edgeExecutionOptionBuilder
                .setWriteMode(WriteModeEnum.INSERT)
                .setFailureHandler(FailureHandlerEnum.FAIL)
                .setMaxRetries(2)
                .setRetryDelayMs(100)
                .build();
        final NebulaEdgeBatchOutputFormat outputFormat =
                new NebulaEdgeBatchOutputFormat(graphProvider, metaProvider, options);

        final Row row = new Row(3);
        row.setField(0, "222");
        row.setField(1, "333");
        row.setField(2, "abc");

        writeRecords(outputFormat, row);
    }

    @Test
    public void testWriteEdgeIgnoreInvalid() throws IOException {
        final EdgeExecutionOptions options = edgeExecutionOptionBuilder
                .setWriteMode(WriteModeEnum.INSERT)
                .setFailureHandler(FailureHandlerEnum.IGNORE)
                .setMaxRetries(2)
                .setRetryDelayMs(100)
                .build();
        final NebulaEdgeBatchOutputFormat outputFormat =
                new NebulaEdgeBatchOutputFormat(graphProvider, metaProvider, options);

        final Row row1 = new Row(3);
        row1.setField(0, "222");
        row1.setField(1, "333");
        row1.setField(2, "abc");
        final Row row2 = new Row(3);
        row2.setField(0, "222");
        row2.setField(1, "333");
        row2.setField(2, 25);

        writeRecords(outputFormat, row1, row2);
        executeNGql("USE flink_sink");
        check(
                Arrays.asList(rowOf(valueOf(25))),
                "GO FROM \"222\" OVER follow YIELD properties(edge).degree"
        );
    }

}
