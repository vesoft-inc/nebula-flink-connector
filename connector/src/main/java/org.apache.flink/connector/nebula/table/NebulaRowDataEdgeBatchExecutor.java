/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.table;

import java.sql.SQLException;
import java.util.Map;
import org.apache.flink.connector.nebula.sink.NebulaEdgeBatchExecutor;
import org.apache.flink.connector.nebula.sink.NebulaRowEdgeOutputFormatConverter;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.table.data.RowData;


public class NebulaRowDataEdgeBatchExecutor extends NebulaEdgeBatchExecutor<RowData> {

    private final NebulaRowDataConverter nebulaConverter;

    public NebulaRowDataEdgeBatchExecutor(ExecutionOptions executionOptions,
                                          VidTypeEnum vidType, Map<String,
                                          Integer> schema,
                                          NebulaRowDataConverter nebulaConverter) {
        super(executionOptions, vidType, schema);
        this.nebulaConverter = nebulaConverter;
    }

    @Override
    protected void addToBatch(RowData record) {
        NebulaRowEdgeOutputFormatConverter converter =
                new NebulaRowEdgeOutputFormatConverter((EdgeExecutionOptions) executionOptions,
                        vidType, schema);
        NebulaEdge edge = null;
        try {
            edge = converter.createEdge(
                    this.nebulaConverter.toExternal(record),
                    executionOptions.getPolicy());
        } catch (SQLException e) {
            e.printStackTrace();
        }
        if (edge == null) {
            return;
        }
        nebulaEdgeList.add(edge);
    }
}
