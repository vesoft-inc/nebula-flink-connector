/*
 * Copyright (c) 2025 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import org.apache.flink.connector.nebula.options.ConnectionOptions;
import org.apache.flink.connector.nebula.options.SinkEdgeOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdgeSchema;
import org.apache.flink.types.Row;

public class NebulaEdgeBatchOutputFormat extends NebulaBatchOutputFormat<Row, SinkEdgeOptions> {
    public NebulaEdgeBatchOutputFormat(ConnectionOptions connectionOptions,
                                       SinkEdgeOptions executionOptions) {
        super(connectionOptions, executionOptions);
    }

    @Override
    protected NebulaBatchExecutor<Row> createNebulaBatchExecutor() {
        try {
            NebulaEdgeSchema schema = graphProvider.getEdgeSchema(
                    executionOptions.getGraphName(), executionOptions.getEdgeType());
            return new NebulaEdgeBatchExecutor(executionOptions, schema);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
