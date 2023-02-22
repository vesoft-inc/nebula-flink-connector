/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.connector.nebula.MockData;
import org.apache.flink.connector.nebula.NebulaITTestBase;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.types.Row;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractNebulaOutputFormatTest extends NebulaITTestBase {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractNebulaOutputFormatTest.class);

    @BeforeClass
    public static void beforeAll() {
        initializeNebulaSession();
        initializeNebulaSchema(MockData.createFlinkSinkSpace());
    }

    @AfterClass
    public static void afterAll() {
        closeNebulaSession();
    }

    @Test
    public void testWrite() throws IOException {
        List<String> cols = Arrays.asList("name", "age");
        VertexExecutionOptions executionOptions =
                new VertexExecutionOptions.ExecutionOptionBuilder()
                        .setGraphSpace("flink_sink")
                        .setTag("player")
                        .setFields(cols)
                        .setPositions(Arrays.asList(1, 2))
                        .setIdIndex(0)
                        .setBatchSize(1)
                        .build();

        NebulaClientOptions clientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setMetaAddress(META_ADDRESS)
                .setGraphAddress(GRAPH_ADDRESS)
                .build();
        NebulaGraphConnectionProvider graphProvider =
                new NebulaGraphConnectionProvider(clientOptions);
        NebulaMetaConnectionProvider metaProvider = new NebulaMetaConnectionProvider(clientOptions);
        Row row = new Row(3);
        row.setField(0, 111);
        row.setField(1, "jena");
        row.setField(2, 12);

        NebulaVertexBatchOutputFormat outputFormat =
                new NebulaVertexBatchOutputFormat(graphProvider, metaProvider, executionOptions);

        outputFormat.open(1, 2);
        outputFormat.writeRecord(row);
    }
}
