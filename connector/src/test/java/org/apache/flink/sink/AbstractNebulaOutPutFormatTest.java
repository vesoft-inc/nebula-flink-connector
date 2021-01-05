/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.sink;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaBatchOutputFormat;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.types.Row;
import org.junit.Test;

public class AbstractNebulaOutPutFormatTest extends TestCase {
    @Test
    public void testWrite() throws IOException {
        List<String> cols = Arrays.asList("name", "age");
        ExecutionOptions executionOptions = new VertexExecutionOptions.ExecutionOptionBuilder()
                .setGraphSpace("flinkSink")
                .setTag("player")
                .setFields(cols)
                .setPositions(Arrays.asList(1, 2))
                .setIdIndex(0)
                .setBatch(1)
                .builder();

        NebulaClientOptions clientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setMetaAddress("127.0.0.1:45500")
                .setGraphAddress("127.0.0.1:3699")
                .build();
        NebulaGraphConnectionProvider graphProvider =
                new NebulaGraphConnectionProvider(clientOptions);
        NebulaMetaConnectionProvider metaProvider = new NebulaMetaConnectionProvider(clientOptions);
        Row row = new Row(3);
        row.setField(0, 111);
        row.setField(1, "jena");
        row.setField(2, 12);

        NebulaBatchOutputFormat outPutFormat =
                new NebulaBatchOutputFormat(graphProvider, metaProvider)
                .setExecutionOptions(executionOptions);

        outPutFormat.open(1, 2);
        outPutFormat.writeRecord(row);
    }

}