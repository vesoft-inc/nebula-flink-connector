/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.utils.NebulaNodeSchema;
import org.apache.flink.types.Row;

public class NebulaNodeBatchOutputFormat extends NebulaBatchOutputFormat<Row, SinkNodeOptions> {

    public NebulaNodeBatchOutputFormat(ConnectionOptions connectionOptions,
                                       SinkNodeOptions executionOptions) {
        super(connectionOptions, executionOptions);
    }

    @Override
    protected NebulaBatchExecutor<Row> createNebulaBatchExecutor() {
        try {
            NebulaNodeSchema schema = graphProvider.getNodeSchema(executionOptions.getGraphName(),
                                                                  executionOptions.getNodeType());
            return new NebulaNodeBatchExecutor(executionOptions, schema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
