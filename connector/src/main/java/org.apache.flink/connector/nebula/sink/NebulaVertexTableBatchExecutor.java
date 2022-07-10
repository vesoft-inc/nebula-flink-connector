/* Copyright (c) 2022 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import java.util.Map;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaVertex;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class NebulaVertexTableBatchExecutor extends NebulaVertexBatchExecutor<RowData> {
    private final DynamicTableSink.DataStructureConverter dataStructureConverter;

    public NebulaVertexTableBatchExecutor(
            ExecutionOptions executionOptions,
            VidTypeEnum vidType,
            Map<String, Integer> schema,
            DynamicTableSink.DataStructureConverter converter) {
        super(executionOptions, vidType, schema);
        this.dataStructureConverter = converter;
    }

    @Override
    void addToBatch(RowData record) {
        NebulaRowVertexOutputFormatConverter converter =
                new NebulaRowVertexOutputFormatConverter(
                        (VertexExecutionOptions) executionOptions, vidType, schema);
        NebulaVertex vertex =
                converter.createVertex(
                        (Row) dataStructureConverter.toExternal(record),
                        executionOptions.getPolicy());
        if (vertex == null) {
            return;
        }
        nebulaVertexList.add(vertex);
    }
}
