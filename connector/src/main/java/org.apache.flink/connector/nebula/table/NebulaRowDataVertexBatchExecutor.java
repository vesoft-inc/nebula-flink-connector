/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.table;


import org.apache.flink.connector.nebula.sink.NebulaRowVertexOutputFormatConverter;
import org.apache.flink.connector.nebula.sink.NebulaVertexBatchExecutor;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaVertex;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.table.data.RowData;

import java.sql.SQLException;
import java.util.Map;

public class NebulaRowDataVertexBatchExecutor extends NebulaVertexBatchExecutor<RowData> {
    private final NebulaRowDataConverter nebulaConverter;

    public NebulaRowDataVertexBatchExecutor(ExecutionOptions executionOptions,
                                            VidTypeEnum vidType,
                                            Map<String, Integer> schema,
                                            NebulaRowDataConverter nebulaConverter) {
        super(executionOptions, vidType, schema);
        this.nebulaConverter = nebulaConverter;
    }

    /**
     * put record into buffer
     *
     * @param record represent vertex or edge
     */
    @Override
    protected void addToBatch(RowData record) {
        NebulaRowVertexOutputFormatConverter converter = new NebulaRowVertexOutputFormatConverter(
                (VertexExecutionOptions) executionOptions, vidType, schema);
        NebulaVertex vertex = null;
        try {
            vertex = converter.createVertex(this.nebulaConverter.toExternal(record), executionOptions.getPolicy());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (vertex == null) {
            return;
        }
        nebulaVertexList.add(vertex);
    }
}
