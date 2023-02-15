/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.NebulaPoolConfig;
import com.vesoft.nebula.client.graph.data.HostAddress;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.NebulaPool;
import com.vesoft.nebula.client.graph.net.Session;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.connector.nebula.connection.NebulaClientOptions;
import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.types.Row;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AbstractNebulaOutputFormatTest {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractNebulaOutputFormatTest.class);

    @Test
    public void testWrite() throws IOException {
        mockNebulaData();
        List<String> cols = Arrays.asList("name", "age");
        VertexExecutionOptions executionOptions =
                new VertexExecutionOptions.ExecutionOptionBuilder()
                        .setGraphSpace("flinkSink")
                        .setTag("player")
                        .setFields(cols)
                        .setPositions(Arrays.asList(1, 2))
                        .setIdIndex(0)
                        .setBatchSize(1)
                        .build();

        NebulaClientOptions clientOptions = new NebulaClientOptions
                .NebulaClientOptionsBuilder()
                .setMetaAddress("127.0.0.1:9559")
                .setGraphAddress("127.0.0.1:9669")
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

    private void mockNebulaData() {
        NebulaPoolConfig nebulaPoolConfig = new NebulaPoolConfig();
        nebulaPoolConfig.setMaxConnSize(100);
        List<HostAddress> addresses = Arrays.asList(new HostAddress("127.0.0.1", 9669));
        NebulaPool pool = new NebulaPool();
        Session session = null;
        try {
            pool.init(addresses, nebulaPoolConfig);
            session = pool.getSession("root", "nebula", true);

            String createSpace = "CREATE SPACE IF NOT EXISTS flinkSink(partition_num=10,"
                    + "vid_type=fixed_string(8));"
                    + "USE flinkSink;"
                    + "CREATE TAG IF NOT EXISTS player(name string, age int);";
            ResultSet resp = session.execute(createSpace);

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (!resp.isSucceeded()) {
                LOGGER.error("create flinkSink space failed, " + resp.getErrorMessage());
                assert (false);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
