/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import static org.apache.flink.connector.nebula.TestConstant.graphAddr;
import static org.apache.flink.connector.nebula.TestConstant.passwd;
import static org.apache.flink.connector.nebula.TestConstant.sinkGraph;
import static org.apache.flink.connector.nebula.TestConstant.user;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.connector.nebula.MockData;
import org.apache.flink.connector.nebula.NebulaITTestBase;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
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
        initializeNebulaClient();
        initializeNebulaSchema(MockData.createFlinkSinkGraphType());
        initializeNebulaSchema(MockData.createFlinkSinkGraph());
    }

    @AfterClass
    public static void afterAll() {
        closeGraphProvider();
    }

    @Test
    public void testWrite() throws IOException {
        List<String> cols = Arrays.asList("name", "age");
        SinkNodeOptions sinkNodeOptions = SinkNodeOptions
                .builder()
                .withGraphName(sinkGraph)
                .withNodeType("person")
                .withFlinkFields(Arrays.asList("id", "name", "age"))
                .withNebulaFields(Arrays.asList("col1", "col2", "col3"))
                .withWriteMode(WriteModeEnum.INSERTREPLACE)
                .withBatchSize(10)
                .build();

        ConnectionOptions connectionOptions = ConnectionOptions
                .builder()
                .withGraphAddress(graphAddr)
                .withUser(user)
                .withPassword(passwd)
                .withConnectionTimeout(5000)
                .withRequestTimeout(5000)
                .build();

        Row row = Row.withNames();
        row.setField("id", 111);
        row.setField("name", "jena");
        row.setField("age", 12);

        NebulaNodeBatchOutputFormat outputFormat =
                new NebulaNodeBatchOutputFormat(connectionOptions, sinkNodeOptions);
        outputFormat.open(1, 2);
        outputFormat.writeRecord(row);
    }
}
