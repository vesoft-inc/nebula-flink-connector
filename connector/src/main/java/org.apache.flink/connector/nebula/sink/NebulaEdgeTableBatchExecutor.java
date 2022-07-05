/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import java.util.Map;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

public class NebulaEdgeTableBatchExecutor extends NebulaEdgeBatchExecutor<RowData> {
    private final DynamicTableSink.DataStructureConverter dataStructureConverter;

    public NebulaEdgeTableBatchExecutor(
            ExecutionOptions executionOptions,
            VidTypeEnum vidType,
            Map<String, Integer> schema,
            DynamicTableSink.DataStructureConverter dataStructureConverter) {
        super(executionOptions, vidType, schema);
        this.dataStructureConverter = dataStructureConverter;
    }

    @Override
    void addToBatch(RowData record) {
        NebulaRowEdgeOutputFormatConverter converter =
                new NebulaRowEdgeOutputFormatConverter(
                        (EdgeExecutionOptions) executionOptions, vidType, schema);
        NebulaEdge edge =
                converter.createEdge(
                        (Row) dataStructureConverter.toExternal(record),
                        executionOptions.getPolicy());
        if (edge == null) {
            return;
        }
        nebulaEdgeList.add(edge);
    }
}
