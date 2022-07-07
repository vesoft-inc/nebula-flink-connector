/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.table;

import org.apache.flink.connector.nebula.connection.NebulaGraphConnectionProvider;
import org.apache.flink.connector.nebula.connection.NebulaMetaConnectionProvider;
import org.apache.flink.connector.nebula.sink.NebulaBatchExecutor;
import org.apache.flink.connector.nebula.sink.NebulaBatchOutputFormat;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.util.Map;

public class NebulaRowDataOutputFormat extends NebulaBatchOutputFormat<RowData> {

    private final LogicalType[] logicalTypes;

    public NebulaRowDataOutputFormat(NebulaGraphConnectionProvider graphProvider,
                                     NebulaMetaConnectionProvider metaProvider,
                                     LogicalType[] logicalTypes) {
        super(graphProvider, metaProvider);
        this.logicalTypes = logicalTypes;
    }

    @Override
    protected NebulaBatchExecutor<RowData> getBatchExecutor(VidTypeEnum vidType, boolean isVertex) {
        RowType rowType = RowType.of(logicalTypes);
        NebulaRowDataConverter nebulaConverter = new NebulaRowDataConverter(rowType);
        Map<String, Integer> schema;
        NebulaBatchExecutor<RowData> nebulaBatchExecutor = null;
        if (isVertex) {
            schema = metaProvider.getTagSchema(metaClient, executionOptions.getGraphSpace(),
                    executionOptions.getLabel());
             nebulaBatchExecutor = new NebulaRowDataVertexBatchExecutor(
                     executionOptions, vidType, schema, nebulaConverter);
        } else {
            schema = metaProvider.getEdgeSchema(metaClient, executionOptions.getGraphSpace(),
                    executionOptions.getLabel());
            nebulaBatchExecutor = new NebulaRowDataEdgeBatchExecutor(
                    executionOptions, vidType, schema, nebulaConverter);
        }
        return nebulaBatchExecutor;
    }
}
