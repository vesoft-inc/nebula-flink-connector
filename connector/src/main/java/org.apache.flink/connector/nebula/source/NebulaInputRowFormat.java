/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.source;

import java.io.IOException;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SourceExecutionOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.types.Row;

/**
 * implementation of NebulaInputFormat.
 * Read NebulaGraph data in flink's {@link Row} format.
 * <p>how to use:
 * NebulaInputRowFormat inputFormat = new NebulaInputRowFormat
 * (storageConnectionProvider, vertexExecutionOptions);
 * DataSource dataSource = env.createInput(inputFormat);
 * </p>
 */
public class NebulaInputRowFormat extends NebulaInputFormat<Row> {

    public NebulaInputRowFormat(ConnectionOptions connectionOptions,
                                SourceExecutionOptions executionOptions) {
        super(connectionOptions, executionOptions);
    }

    @Override
    public void open(InputSplit inputSplit) throws IOException {
        super.open(inputSplit);
        super.nebulaConverter = new NebulaRowConverter();
    }
}
