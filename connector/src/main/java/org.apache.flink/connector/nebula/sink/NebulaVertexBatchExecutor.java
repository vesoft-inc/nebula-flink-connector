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
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaVertex;
import org.apache.flink.connector.nebula.utils.NebulaVertices;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaVertexBatchExecutor implements NebulaBatchExecutor<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaVertexBatchExecutor.class);
    private final VertexExecutionOptions executionOptions;
    private final List<NebulaVertex> nebulaVertexList;
    private final NebulaRowVertexOutputFormatConverter converter;

    public NebulaVertexBatchExecutor(VertexExecutionOptions executionOptions,
                                     VidTypeEnum vidType, Map<String, Integer> schema) {
        this.executionOptions = executionOptions;
        this.nebulaVertexList = new ArrayList<>();
        this.converter = new NebulaRowVertexOutputFormatConverter(executionOptions,
                vidType, schema);
    }

    /**
     * put record into buffer
     *
     * @param record represent vertex or edge
     */
    @Override
    public void addToBatch(Row record) {
        NebulaVertex vertex = converter.createVertex(record, executionOptions.getPolicy());
        if (vertex == null) {
            return;
        }
        nebulaVertexList.add(vertex);
    }

    @Override
    public void executeBatch(Session session) {
        if (nebulaVertexList.size() == 0) {
            return;
        }
        NebulaVertices nebulaVertices = new NebulaVertices(executionOptions.getLabel(),
                executionOptions.getFields(), nebulaVertexList, executionOptions.getPolicy());
        // generate the write ngql statement
        String statement = null;
        switch (executionOptions.getWriteMode()) {
            case INSERT:
                statement = nebulaVertices.getInsertStatement();
                break;
            case UPDATE:
                statement = nebulaVertices.getUpdateStatement();
                break;
            case DELETE:
                statement = nebulaVertices.getDeleteStatement();
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
            nebulaVertexList.clear();
            return;
        }

        if (execResult.isSucceeded()) {
            LOG.debug("write success");
        } else {
            LOG.error("write data failed: {}", execResult.getErrorMessage());
            nebulaVertexList.clear();
            return;
        }
        nebulaVertexList.clear();
    }
}
