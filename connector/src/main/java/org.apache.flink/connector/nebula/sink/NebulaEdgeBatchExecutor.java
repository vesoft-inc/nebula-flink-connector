/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.net.Session;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.NebulaEdges;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaEdgeBatchExecutor extends NebulaBatchExecutor<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaEdgeBatchExecutor.class);
    private final EdgeExecutionOptions executionOptions;
    private final List<NebulaEdge> nebulaEdgeList;
    private final NebulaRowEdgeOutputFormatConverter converter;

    public NebulaEdgeBatchExecutor(EdgeExecutionOptions executionOptions,
                                   VidTypeEnum vidType, Map<String, Integer> schema) {
        this.executionOptions = executionOptions;
        this.nebulaEdgeList = new ArrayList<>();
        this.converter = new NebulaRowEdgeOutputFormatConverter(executionOptions, vidType, schema);
    }

    /**
     * put record into buffer
     */
    @Override
    public void addToBatch(Row record) {
        NebulaEdge edge = converter.createEdge(record, executionOptions.getPolicy());
        if (edge == null) {
            return;
        }
        nebulaEdgeList.add(edge);
    }

    @Override
    public void clearBatch() {
        nebulaEdgeList.clear();
    }

    @Override
    public boolean isBatchEmpty() {
        return nebulaEdgeList.isEmpty();
    }

    @Override
    public void executeBatch(Session session) throws IOException {
        if (isBatchEmpty()) {
            return;
        }
        NebulaEdges nebulaEdges = new NebulaEdges(executionOptions.getLabel(),
                executionOptions.getFields(), nebulaEdgeList, executionOptions.getPolicy(),
                executionOptions.getPolicy());
        // generate the write ngql statement
        String statement = null;
        switch (executionOptions.getWriteMode()) {
            case INSERT:
                statement = nebulaEdges.getInsertStatement();
                break;
            case UPDATE:
                statement = nebulaEdges.getUpdateStatement();
                break;
            case DELETE:
                statement = nebulaEdges.getDeleteStatement();
                break;
            default:
                throw new IllegalArgumentException("write mode is not supported");
        }
        executeStatement(session, statement);
        clearBatch();
    }
}
