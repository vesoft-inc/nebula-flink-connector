/* Copyright (c) 2021 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.Session;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaEdge;
import org.apache.flink.connector.nebula.utils.NebulaEdges;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaEdgeBatchExecutor<T> extends NebulaBatchExecutor<T> {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaEdgeBatchExecutor.class);
    private final List<NebulaEdge> nebulaEdgeList;

    public NebulaEdgeBatchExecutor(ExecutionOptions executionOptions,
                                   VidTypeEnum vidType, Map<String, Integer> schema) {
        super(executionOptions, vidType, schema);
        nebulaEdgeList = new ArrayList<>();
    }

    /**
     * put record into buffer
     */
    @Override
    void addToBatch(T record) {
        NebulaRowEdgeOutputFormatConverter converter =
                new NebulaRowEdgeOutputFormatConverter((EdgeExecutionOptions) executionOptions,
                        vidType, schema);
        NebulaEdge edge = converter.createEdge((Row) record, executionOptions.getPolicy());
        if (edge == null) {
            return;
        }
        nebulaEdgeList.add(edge);
    }

    @Override
    String executeBatch(Session session) {
        if (nebulaEdgeList.size() == 0) {
            return null;
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
        LOG.debug("write statement={}", statement);

        // execute ngql statement
        ResultSet execResult = null;
        try {
            execResult = session.execute(statement);
        } catch (Exception e) {
            LOG.error("write data error, ", e);
            nebulaEdgeList.clear();
            return statement;
        }

        if (execResult.isSucceeded()) {
            LOG.debug("write success");
        } else {
            LOG.error("write data failed: {}", execResult.getErrorMessage());
            nebulaEdgeList.clear();
            return statement;
        }
        nebulaEdgeList.clear();
        return null;
    }
}
