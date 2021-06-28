/* Copyright (c) 2020 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.net.Session;
import java.util.Map;
import org.apache.flink.connector.nebula.statement.EdgeExecutionOptions;
import org.apache.flink.connector.nebula.statement.ExecutionOptions;
import org.apache.flink.connector.nebula.statement.VertexExecutionOptions;
import org.apache.flink.connector.nebula.utils.NebulaConstant;
import org.apache.flink.connector.nebula.utils.VidTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaBatchExecutor<T> {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaBatchExecutor.class);

    private final ExecutionOptions executionOptions;
    private final NebulaBufferedRow nebulaBufferedRow;
    private final boolean isVertex;
    private final Map<String, Integer> schema;
    private final VidTypeEnum vidType;

    public NebulaBatchExecutor(ExecutionOptions executionOptions, boolean isVertex,
                               VidTypeEnum vidType, Map<String, Integer> schema) {
        this.executionOptions = executionOptions;
        this.nebulaBufferedRow = new NebulaBufferedRow();
        this.isVertex = isVertex;
        this.vidType = vidType;
        this.schema = schema;
    }

    /**
     * put record into buffer
     *
     * @param record represent vertex or edge
     */
    void addToBatch(T record) {
        NebulaOutputFormatConverter converter;
        if (isVertex) {
            converter = new NebulaRowVertexOutputFormatConverter(
                    (VertexExecutionOptions) executionOptions, vidType, schema);
        } else {
            converter = new NebulaRowEdgeOutputFormatConverter(
                    (EdgeExecutionOptions) executionOptions, vidType, schema);
        }
        String value = converter.createValue(record, executionOptions.getPolicy());
        if (value == null) {
            return;
        }
        nebulaBufferedRow.putRow(value);
    }

    /**
     * execute the insert statement
     *
     * @param session graph session
     */
    String executeBatch(Session session) {
        String propNames = String.join(NebulaConstant.COMMA, executionOptions.getFields());
        String values = String.join(NebulaConstant.COMMA, nebulaBufferedRow.getRows());
        String exec = String.format(NebulaConstant.BATCH_INSERT_TEMPLATE,
                executionOptions.getDataType(), executionOptions.getLabel(), propNames, values);
        LOG.info("insert statement={}", exec);
        ResultSet execResult = null;
        try {
            execResult = session.execute(exec);
        } catch (Exception e) {
            LOG.error("insert error:", e);
            nebulaBufferedRow.clean();
            return exec;
        }

        if (execResult.isSucceeded()) {
            LOG.debug("insert success");
        } else {
            LOG.error("insert failed: {}", execResult.getErrorMessage());
            nebulaBufferedRow.clean();
            return exec;
        }
        nebulaBufferedRow.clean();
        return null;
    }
}
