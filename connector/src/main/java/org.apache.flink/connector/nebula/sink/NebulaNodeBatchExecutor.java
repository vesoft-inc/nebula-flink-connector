/*
 * Copyright (c) 2026 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package org.apache.flink.connector.nebula.sink;

import com.vesoft.nebula.driver.graph.ErrorCode;
import com.vesoft.nebula.driver.graph.data.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.connector.nebula.connection.GraphProvider;
import org.apache.flink.connector.nebula.options.SinkNodeOptions;
import org.apache.flink.connector.nebula.utils.NebulaNode;
import org.apache.flink.connector.nebula.utils.NebulaNodeSchema;
import org.apache.flink.connector.nebula.utils.NebulaNodes;
import org.apache.flink.connector.nebula.utils.WriteModeEnum;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NebulaNodeBatchExecutor extends AbstractNebulaRetryableBatchExecutor<NebulaNode>
        implements NebulaBatchExecutor<Row> {
    private static final Logger LOG = LoggerFactory.getLogger(NebulaNodeBatchExecutor.class);

    private final SinkNodeOptions                    executionOptions;
    private final List<NebulaNode>                   nebulaVertexList;
    private final NebulaRowNodeOutputFormatConverter converter;
    private final NebulaNodeSchema                   schema;

    public NebulaNodeBatchExecutor(SinkNodeOptions executionOptions,
                                   NebulaNodeSchema schema) {
        this.executionOptions = executionOptions;
        this.nebulaVertexList = new ArrayList<>();
        this.schema = schema;
        this.converter = new NebulaRowNodeOutputFormatConverter(executionOptions, schema);
    }

    /**
     * put record into buffer
     *
     * @param record represent vertex or edge
     */
    @Override
    public void addToBatch(Row record) {
        NebulaNode vertex = converter.createNode(record);
        if (vertex == null) {
            LOG.error(">>>>> vertex is null. maybe the row is invalid.");
            return;
        }
        nebulaVertexList.add(vertex);
    }

    @Override
    public String executeBatch(GraphProvider graphProvider) {
        if (nebulaVertexList.isEmpty()) {
            return null;
        }
        try {
            return writeEntities(new ArrayList<>(nebulaVertexList), graphProvider);
        } finally {
            nebulaVertexList.clear();
        }
    }

    @Override
    protected String getGql(List<NebulaNode> nodes) {
        NebulaNodes nebulaNodes = new NebulaNodes(schema, nodes);
        if (executionOptions.hasCustomGqlTemplate()) {
            Map<String, String> values = new HashMap<>();
            values.put("TABLE",
                       nebulaNodes.buildTableClause(executionOptions.getFlinkFields(),
                                                    executionOptions.getNebulaFields()));
            values.put("GRAPH", executionOptions.getGraphName());
            values.put("TYPE", executionOptions.getNodeType());
            values.put("LABEL", executionOptions.getNodeType());
            values.put("WRITE_MODE", executionOptions.getWriteMode().name());
            values.put("WRITE_MODE_NGQL", executionOptions.getWriteMode().getMode());
            return NebulaGqlTemplateEngine.render(executionOptions.getGqlTemplate(), values);
        }

        switch (executionOptions.getWriteMode()) {
            case INSERT:
            case INSERTIGNORE:
            case INSERTREPLACE:
            case INSERTUPDATE:
                return nebulaNodes.getInsertStatement(executionOptions.getGraphName(),
                                                      executionOptions.getWriteMode(),
                                                      executionOptions.getFlinkFields(),
                                                      executionOptions.getNebulaFields());
            case UPDATE:
                return nebulaNodes.getUpdateStatement(executionOptions.getGraphName(),
                                                      executionOptions.getFlinkFields(),
                                                      executionOptions.getNebulaFields());
            case DELETE:
            case DETACHDELETE:
                return nebulaNodes.getDeleteStatement(executionOptions.getGraphName(),
                                                      executionOptions.getWriteMode(),
                                                      executionOptions.getFlinkFields(),
                                                      executionOptions.getNebulaFields());
            default:
                throw new IllegalArgumentException("write mode is not supported");
        }
    }

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    @Override
    protected String getEntityName() {
        return "node";
    }

    @Override
    protected String getWriteTarget() {
        return executionOptions.getNodeType();
    }

    @Override
    protected WriteModeEnum getWriteMode() {
        return executionOptions.getWriteMode();
    }

    @Override
    protected boolean throwErrorWhenFailed() {
        return executionOptions.throwErrorWhenFailed();
    }

    @Override
    protected int getRetryTimes() {
        return executionOptions.getRetryTimes();
    }

    @Override
    protected long getRetryIntervalMs() {
        return executionOptions.getIntervalMs();
    }

    @Override
    protected boolean isAlreadyExist(ResultSet resultSet) {
        return resultSet.getErrorCode() == ErrorCode.NODE_ALREADY_EXIST;
    }

    @Override
    protected long getAffectedCount(ResultSet resultSet) {
        return resultSet.getExtraInfo() == null ? 0L : resultSet.getExtraInfo().getAffectedNodes();
    }
}
